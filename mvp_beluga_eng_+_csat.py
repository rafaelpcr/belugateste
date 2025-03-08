import mysql.connector
from datetime import datetime, timedelta
import logging
import json
from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
import traceback
import time
import numpy as np

# Configuração básica de logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('radar.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('radar_app')

# Carregar variáveis de ambiente
load_dotenv()

app = Flask(__name__)

def convert_radar_data(raw_data):
    """Converte dados do radar para o formato do banco"""
    try:
        logger.debug(f"Convertendo dados brutos: {raw_data}")
        
        # Processar dados diretamente do formato do radar
        converted_data = {
            'device_id': 'RADAR_1',  # ID fixo para identificar o radar
            'x_point': float(raw_data.get('x', 0)),
            'y_point': float(raw_data.get('y', 0)),
            'move_speed': float(raw_data.get('move_speed', 0)),
            'heart_rate': float(raw_data.get('heart_rate', 0)) if raw_data.get('heart_rate') is not None else None,
            'breath_rate': float(raw_data.get('breath_rate', 0)) if raw_data.get('breath_rate') is not None else None
        }
        
        logger.info(f"✅ Dados convertidos com sucesso: {converted_data}")
        return converted_data, None
    except Exception as e:
        logger.error(f"❌ Erro ao converter dados: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return None, f"Erro ao converter dados: {str(e)}"

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.last_sequence = 0
        self.last_move_speed = None
        self.connect_with_retry()
        
    def connect_with_retry(self, max_attempts=5):
        """Tenta conectar ao banco com retry"""
        attempt = 0
        while attempt < max_attempts:
            try:
                attempt += 1
                logger.info(f"Tentativa {attempt} de {max_attempts} para conectar ao banco...")
                
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                
                # Usando driver puro Python e configurações SSL mais simples
                self.conn = mysql.connector.connect(
                    host="168.75.89.11",
                    user="belugaDB",
                    password="Rpcr@300476",
                    database="Beluga_Analytics",
                    port=3306,
                    use_pure=True,  # Força o uso do driver puro Python
                    ssl_disabled=True  # Desabilita SSL temporariamente
                )
                self.cursor = self.conn.cursor(dictionary=True)
                
                # Testar conexão
                self.cursor.execute("SELECT 1")
                self.cursor.fetchone()
                
                logger.info("✅ Conexão estabelecida com sucesso!")
                self.initialize_database()
                return True
                
            except Exception as e:
                logger.error(f"❌ Tentativa {attempt} falhou: {str(e)}")
                if attempt == max_attempts:
                    logger.error("Todas as tentativas de conexão falharam!")
                    raise
                time.sleep(2)
        return False

    def initialize_database(self):
        """Inicializa o banco de dados"""
        try:
            # Verificar se a tabela existe
            self.cursor.execute("SHOW TABLES LIKE 'radar_dados'")
            table_exists = self.cursor.fetchone()
            
            if not table_exists:
                # Criar tabela se não existir
                logger.info("Criando tabela radar_dados...")
                self.cursor.execute("""
                    CREATE TABLE radar_dados (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        x_point FLOAT,
                        y_point FLOAT,
                        move_speed FLOAT,
                        heart_rate FLOAT,
                        breath_rate FLOAT,
                        satisfaction_score FLOAT,
                        satisfaction_class VARCHAR(20),
                        is_engaged BOOLEAN,
                        engagement_duration INT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Tabela radar_dados criada com sucesso!")
            else:
                # Verificar e adicionar colunas que estão faltando
                logger.info("Verificando colunas da tabela radar_dados...")
                
                # Verificar se as colunas existem
                self.cursor.execute("DESCRIBE radar_dados")
                columns = self.cursor.fetchall()
                existing_columns = [column['Field'] for column in columns]
                
                logger.info(f"Colunas existentes: {existing_columns}")
                
                # Colunas que devem existir
                required_columns = {
                    'satisfaction_score': 'ADD COLUMN satisfaction_score FLOAT',
                    'satisfaction_class': 'ADD COLUMN satisfaction_class VARCHAR(20)',
                    'is_engaged': 'ADD COLUMN is_engaged BOOLEAN',
                    'engagement_duration': 'ADD COLUMN engagement_duration INT'
                }
                
                # Adicionar colunas faltantes
                for column, add_command in required_columns.items():
                    if column not in existing_columns:
                        logger.info(f"Adicionando coluna {column}...")
                        try:
                            self.cursor.execute(f"ALTER TABLE radar_dados {add_command}")
                            logger.info(f"Coluna {column} adicionada com sucesso!")
                        except Exception as e:
                            logger.error(f"Erro ao adicionar coluna {column}: {str(e)}")
            
            self.conn.commit()
            logger.info("✅ Banco de dados inicializado com sucesso!")
            
        except Exception as e:
            logger.error(f"❌ Erro ao inicializar banco: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def insert_data(self, data, analytics_data=None):
        """Insere dados no banco"""
        try:
            logger.info("="*50)
            logger.info("Iniciando inserção de dados no banco...")
            
            # Verificar conexão antes de inserir
            if not self.conn or not self.conn.is_connected():
                logger.info("Conexão não disponível, tentando reconectar...")
                self.connect_with_retry()
            
            # Valores padrão para analytics
            satisfaction_score = None
            satisfaction_class = None
            is_engaged = False
            engagement_duration = 0
            
            # Extrair dados de analytics se disponíveis
            if analytics_data:
                if 'satisfaction' in analytics_data:
                    satisfaction_score = analytics_data['satisfaction'].get('score')
                    satisfaction_class = analytics_data['satisfaction'].get('classification')
                is_engaged = bool(analytics_data.get('engaged', False))
                engagement_duration = int(analytics_data.get('engagement_duration', 0))
            
            # Preparar query com campos adicionais
            query = """
                INSERT INTO radar_dados
                (x_point, y_point, move_speed, heart_rate, breath_rate, 
                 satisfaction_score, satisfaction_class, is_engaged, engagement_duration)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Preparar parâmetros
            params = (
                float(data.get('x_point', 0)),
                float(data.get('y_point', 0)),
                float(data.get('move_speed', 0)),
                float(data.get('heart_rate', 0)) if data.get('heart_rate') is not None else None,
                float(data.get('breath_rate', 0)) if data.get('breath_rate') is not None else None,
                float(satisfaction_score) if satisfaction_score is not None else None,
                satisfaction_class,
                is_engaged,
                engagement_duration
            )
            
            logger.info(f"Query SQL: {query}")
            logger.info(f"Parâmetros: {params}")
            
            # Executar inserção
            self.cursor.execute(query, params)
            self.conn.commit()
            
            logger.info("✅ Dados inseridos com sucesso!")
            logger.info("="*50)
            return True
            
        except mysql.connector.Error as e:
            logger.error("="*50)
            logger.error(f"❌ Erro MySQL ao inserir dados: {str(e)}")
            logger.error(f"Código do erro: {e.errno}")
            logger.error(f"Mensagem SQL State: {e.sqlstate}")
            logger.error(f"Mensagem completa: {e.msg}")
            logger.error(f"Dados que tentamos inserir: {data}")
            logger.error(f"Analytics: {analytics_data}")
            logger.error("="*50)
            
            if self.conn:
                try:
                    self.conn.rollback()
                    logger.info("Rollback realizado com sucesso")
                except Exception as rollback_error:
                    logger.error(f"Erro ao fazer rollback: {str(rollback_error)}")
            
            # Tentar reconectar em caso de erro de conexão
            if e.errno in [2006, 2013, 2055]:  # Códigos de erro de conexão
                logger.info("Tentando reconectar após erro de conexão...")
                self.connect_with_retry()
            
            raise
            
        except Exception as e:
            logger.error("="*50)
            logger.error(f"❌ Erro genérico ao inserir dados: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            logger.error(f"Dados que tentamos inserir: {data}")
            logger.error(f"Analytics: {analytics_data}")
            logger.error("="*50)
            raise

    def get_last_records(self, limit=5):
        """Obtém últimos registros"""
        try:
            if not self.conn or not self.conn.is_connected():
                self.connect_with_retry()
                
            query = """
                SELECT * FROM radar_dados
                ORDER BY timestamp DESC 
                LIMIT %s
            """
            
            self.cursor.execute(query, (limit,))
            records = self.cursor.fetchall()
            
            # Converter datetime para string
            for record in records:
                if isinstance(record['timestamp'], datetime):
                    record['timestamp'] = record['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            return records
        except Exception as e:
            logger.error(f"Erro ao buscar registros: {str(e)}")
            return []

# Instância global do gerenciador de banco de dados
try:
    logger.info("Iniciando DatabaseManager...")
    db_manager = DatabaseManager()
    logger.info("✅ DatabaseManager iniciado com sucesso!")
except Exception as e:
    logger.error(f"❌ Erro ao criar instância do DatabaseManager: {e}")
    logger.error(traceback.format_exc())
    db_manager = None

class AnalyticsManager:
    def __init__(self):
        # Constantes para engajamento
        self.ENGAGEMENT_TIME_THRESHOLD = 5  # segundos
        self.MOVEMENT_THRESHOLD = 0.1  # limite para considerar "parado"
        
        # Constantes para satisfação
        self.RESP_RATE_NORMAL_MIN = 6
        self.RESP_RATE_NORMAL_MAX = 12
        self.RESP_RATE_ANXIETY = 20
        
        # Pesos para o cálculo de satisfação
        self.WEIGHT_HEART_RATE = 0.6  # α
        self.WEIGHT_RESP_RATE = 0.4   # β
        
    def calculate_engagement(self, records):
        """
        Calcula engajamento baseado no histórico de registros
        Retorna True se pessoa ficou parada por mais de 5 segundos
        """
        if not records or len(records) < 2:
            return False
            
        # Filtrar registros válidos
        valid_records = []
        for record in records:
            if (record.get('move_speed') is not None and 
                record.get('timestamp') is not None):
                valid_records.append(record)
        
        if len(valid_records) < 2:
            return False
            
        # Organizar registros por timestamp
        try:
            # Tentar ordenar por timestamp
            sorted_records = sorted(valid_records, key=lambda x: x['timestamp'])
        except (TypeError, ValueError):
            logger.error("Erro ao ordenar registros por timestamp")
            return False
        
        # Verificar sequência de registros "parados"
        start_time = None
        for record in sorted_records:
            try:
                move_speed = float(record.get('move_speed', 999))
                
                if move_speed <= self.MOVEMENT_THRESHOLD:
                    # Pessoa está parada
                    timestamp = record.get('timestamp')
                    
                    # Converter timestamp para datetime se for string
                    current_time = None
                    if isinstance(timestamp, str):
                        try:
                            current_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            logger.error(f"Formato de timestamp inválido: {timestamp}")
                            continue
                    elif isinstance(timestamp, datetime):
                        current_time = timestamp
                    else:
                        logger.error(f"Tipo de timestamp não suportado: {type(timestamp)}")
                        continue
                    
                    if start_time is None:
                        start_time = current_time
                    else:
                        duration = (current_time - start_time).total_seconds()
                        logger.info(f"Duração parado: {duration} segundos")
                        if duration >= self.ENGAGEMENT_TIME_THRESHOLD:
                            return True
                else:
                    # Pessoa está se movendo
                    start_time = None
            except Exception as e:
                logger.error(f"Erro ao processar registro para engajamento: {str(e)}")
                continue
                
        return False
        
    def calculate_satisfaction(self, heart_rate, breath_rate):
        """
        Calcula nível de satisfação baseado em batimentos cardíacos e respiração
        Retorna: score (0-100) e classificação ('POSITIVA', 'NEUTRA', 'NEGATIVA')
        """
        # Normalizar respiração (0-100)
        resp_score = 0
        if breath_rate:
            if self.RESP_RATE_NORMAL_MIN <= breath_rate <= self.RESP_RATE_NORMAL_MAX:
                resp_score = 100
            elif breath_rate > self.RESP_RATE_ANXIETY:
                resp_score = 0
            else:
                # Interpolação linear para valores intermediários
                if breath_rate < self.RESP_RATE_NORMAL_MIN:
                    resp_score = (breath_rate / self.RESP_RATE_NORMAL_MIN) * 100
                else:
                    resp_score = max(0, 100 - ((breath_rate - self.RESP_RATE_NORMAL_MAX) / 
                                             (self.RESP_RATE_ANXIETY - self.RESP_RATE_NORMAL_MAX)) * 100)
        
        # Normalizar batimentos (assumindo faixa normal 60-100 bpm)
        heart_score = 0
        if heart_rate:
            if 60 <= heart_rate <= 100:
                heart_score = 100
            elif heart_rate > 100:
                heart_score = max(0, 100 - ((heart_rate - 100) / 20) * 100)
            else:
                heart_score = max(0, (heart_rate / 60) * 100)
        
        # Calcular score final
        final_score = (self.WEIGHT_HEART_RATE * heart_score + 
                      self.WEIGHT_RESP_RATE * resp_score)
        
        # Classificar satisfação
        if final_score >= 70:
            classification = 'POSITIVA'
        elif final_score <= 40:
            classification = 'NEGATIVA'
        else:
            classification = 'NEUTRA'
            
        return {
            'score': round(final_score, 2),
            'classification': classification,
            'heart_score': round(heart_score, 2),
            'resp_score': round(resp_score, 2)
        }

# Instância global do analytics manager
analytics_manager = AnalyticsManager()

@app.route('/radar/data', methods=['POST'])
def receive_radar_data():
    """Endpoint para receber dados do radar"""
    try:
        logger.info("📡 Requisição POST recebida em /radar/data")
        logger.info(f"Headers: {request.headers}")
        
        # Verificar Content-Type
        if not request.is_json:
            logger.error("❌ Content-Type não é application/json")
            return jsonify({
                "status": "error",
                "message": "Content-Type deve ser application/json"
            }), 400
        
        # Obter dados
        raw_data = request.get_json()
        logger.info(f"Dados recebidos: {raw_data}")
        
        # Verificar se há dados
        if not raw_data:
            logger.error("❌ Nenhum dado recebido")
            return jsonify({
                "status": "error",
                "message": "Nenhum dado recebido"
            }), 400
        
        # Converter dados
        converted_data, error = convert_radar_data(raw_data)
        if error:
            logger.error(f"❌ Erro ao converter dados: {error}")
            return jsonify({
                "status": "error",
                "message": error
            }), 400
        
        # Verificar se o banco está disponível
        if not db_manager:
            logger.error("❌ Banco de dados não disponível")
            return jsonify({
                "status": "error",
                "message": "Banco de dados não disponível"
            }), 500
        
        # Inicializar analytics
        analytics_data = {
            "satisfaction": {
                "score": 0,
                "classification": "NEUTRA",
                "heart_score": 0,
                "resp_score": 0
            },
            "engaged": False,
            "engagement_duration": 0
        }
        
        # Calcular satisfação se houver dados suficientes
        if converted_data.get('heart_rate') is not None or converted_data.get('breath_rate') is not None:
            try:
                satisfaction_data = analytics_manager.calculate_satisfaction(
                    converted_data.get('heart_rate'),
                    converted_data.get('breath_rate')
                )
                analytics_data["satisfaction"] = satisfaction_data
            except Exception as e:
                logger.error(f"❌ Erro ao calcular satisfação: {str(e)}")
                logger.error(traceback.format_exc())
        
        # Verificar engajamento se houver dados suficientes
        try:
            last_records = db_manager.get_last_records(10)  # Últimos 10 registros para análise
            if last_records:
                is_engaged = analytics_manager.calculate_engagement(last_records)
                analytics_data["engaged"] = is_engaged
        except Exception as e:
            logger.error(f"❌ Erro ao calcular engajamento: {str(e)}")
            logger.error(traceback.format_exc())
        
        # Inserir dados
        try:
            db_manager.insert_data(converted_data, analytics_data)
        except Exception as e:
            logger.error(f"❌ Erro ao inserir dados: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({
                "status": "error",
                "message": f"Erro ao inserir dados: {str(e)}"
            }), 500
        
        # Adicionar métricas à resposta
        response_data = {
            "status": "success",
            "message": "Dados processados com sucesso",
            "processed_data": converted_data,
            "analytics": analytics_data
        }
        
        return jsonify(response_data)

    except Exception as e:
        logger.error(f"❌ Erro ao processar requisição: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno: {str(e)}"
        }), 500

@app.route('/radar/status', methods=['GET'])
def get_status():
    """Endpoint para verificar status"""
    try:
        status = {
            "server": "online",
            "database": "offline",
            "last_records": None
        }

        if db_manager and db_manager.conn and db_manager.conn.is_connected():
            status["database"] = "online"
            status["last_records"] = db_manager.get_last_records(5)

        return jsonify(status)
    except Exception as e:
        logger.error(f"Erro ao verificar status: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3000))
    host = os.getenv("HOST", "0.0.0.0")
    
    print("\n" + "="*50)
    print("🚀 Servidor Radar iniciando...")
    print(f"📡 Endpoint dados: http://{host}:{port}/radar/data")
    print(f"ℹ️  Endpoint status: http://{host}:{port}/radar/status")
    print("⚡ Use Ctrl+C para encerrar")
    print("="*50 + "\n")
    
    app.run(host=host, port=port, debug=True) 
