import mysql.connector
from datetime import datetime
import logging
import json
from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
import traceback
import time

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

# Configurações mais simples do MySQL
db_config = {
    "host": "168.75.89.11",
    "user": "belugaDB",
    "password": "Rpcr@300476",
    "database": "Beluga_Analytics",
    "port": 3306,
    "connect_timeout": 60,  # Aumentado timeout
    "use_pure": True
}

app = Flask(__name__)

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
                
                # Fechar conexão anterior se existir
                if self.conn:
                    try:
                        self.conn.close()
                        logger.info("Conexão anterior fechada")
                    except:
                        pass
                
                # Criar nova conexão
                self.conn = mysql.connector.connect(**db_config)
                self.cursor = self.conn.cursor(dictionary=True)
                
                # Testar conexão
                self.cursor.execute("SELECT 1")
                self.cursor.fetchone()
                
                logger.info("✅ Conexão estabelecida com sucesso!")
                
                # Inicializar banco se necessário
                self.initialize_database()
                return True
                
            except Exception as e:
                logger.error(f"❌ Tentativa {attempt} falhou: {str(e)}")
                if attempt == max_attempts:
                    logger.error("Todas as tentativas de conexão falharam!")
                    raise
                time.sleep(2)  # Espera 2 segundos antes da próxima tentativa
        return False

    def initialize_database(self):
        """Inicializa o banco de dados"""
        try:
            # Criar tabela
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS radar_interacoes (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    device_id VARCHAR(50),
                    x_point FLOAT,
                    y_point FLOAT,
                    move_speed FLOAT,
                    heart_rate FLOAT,
                    breath_rate FLOAT,
                    sequencia_engajamento INT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Criar view
            self.cursor.execute("""
                CREATE OR REPLACE VIEW engajamento AS
                SELECT 
                    sequencia_engajamento, 
                    COUNT(*) / 5 AS segundos_parado 
                FROM radar_interacoes 
                WHERE move_speed = 0 
                GROUP BY sequencia_engajamento
            """)
            
            self.conn.commit()
            logger.info("✅ Banco de dados inicializado com sucesso!")
            
            # Obter última sequência
            self.cursor.execute("SELECT MAX(sequencia_engajamento) as last_seq FROM radar_interacoes")
            result = self.cursor.fetchone()
            self.last_sequence = result['last_seq'] if result and result['last_seq'] is not None else 0
            
        except Exception as e:
            logger.error(f"❌ Erro ao inicializar banco: {str(e)}")
            raise

    def ensure_connection(self):
        """Verifica e reconecta se necessário"""
        try:
            if not self.conn or not self.conn.is_connected():
                logger.warning("Conexão perdida. Tentando reconectar...")
                return self.connect_with_retry()
            return True
        except Exception as e:
            logger.error(f"Erro ao verificar conexão: {str(e)}")
            return False

    def execute_query(self, query, params=None):
        """Executa uma query com retry"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not self.ensure_connection():
                    raise Exception("Não foi possível estabelecer conexão")
                
                self.cursor.execute(query, params)
                if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                    self.conn.commit()
                return self.cursor.fetchall() if query.strip().upper().startswith('SELECT') else None
                
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt + 1}: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(1)
                self.connect_with_retry()

    def insert_data(self, data):
        """Insere dados no banco"""
        try:
            # Validar e converter dados
            move_speed = float(data['move_speed'])
            x_point = float(data['x_point'])
            y_point = float(data['y_point'])
            heart_rate = float(data.get('heart_rate', 0)) or None
            breath_rate = float(data.get('breath_rate', 0)) or None
            device_id = str(data.get('device_id', 'UNKNOWN'))
            
            # Calcular sequência
            if self.last_move_speed is None:
                self.last_move_speed = move_speed
            elif (self.last_move_speed == 0 and move_speed > 0) or (self.last_move_speed > 0 and move_speed == 0):
                self.last_sequence += 1
            self.last_move_speed = move_speed
            
            # Inserir dados
            query = """
                INSERT INTO radar_interacoes
                (device_id, x_point, y_point, move_speed, heart_rate, breath_rate, sequencia_engajamento)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = (device_id, x_point, y_point, move_speed, heart_rate, breath_rate, self.last_sequence)
            
            logger.info(f"Inserindo dados: {params}")
            self.execute_query(query, params)
            logger.info("✅ Dados inseridos com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao inserir dados: {str(e)}")
            logger.error(f"Dados recebidos: {data}")
            raise

    def get_last_records(self, limit=5):
        """Obtém últimos registros"""
        try:
            query = """
                SELECT 
                    r.*,
                    COALESCE(e.segundos_parado, 0) as segundos_parado
                FROM radar_interacoes r
                LEFT JOIN engajamento e ON r.sequencia_engajamento = e.sequencia_engajamento
                ORDER BY r.timestamp DESC 
                LIMIT %s
            """
            records = self.execute_query(query, (limit,))
            
            # Converter datetime para string
            for record in records:
                record['timestamp'] = record['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            return records
        except Exception as e:
            logger.error(f"Erro ao buscar registros: {str(e)}")
            return []

    def get_engagement_stats(self):
        """Obtém estatísticas de engajamento"""
        try:
            query = """
                SELECT 
                    sequencia_engajamento,
                    segundos_parado,
                    (SELECT COUNT(*) FROM radar_interacoes WHERE sequencia_engajamento = e.sequencia_engajamento) as total_registros
                FROM engajamento e
                ORDER BY sequencia_engajamento DESC
                LIMIT 10
            """
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas: {str(e)}")
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

def validate_data(data):
    """Valida os dados recebidos"""
    try:
        required_fields = ['x_point', 'y_point', 'move_speed']
        
        # Verifica campos obrigatórios
        if not all(field in data for field in required_fields):
            missing = [field for field in required_fields if field not in data]
            return False, f"Campos obrigatórios faltando: {missing}"

        # Verifica tipos de dados
        for field in ['x_point', 'y_point', 'move_speed']:
            try:
                float(data[field])
            except (ValueError, TypeError):
                return False, f"Campo {field} deve ser um número válido"

        # Verifica campos opcionais
        for field in ['heart_rate', 'breath_rate']:
            if field in data and data[field] is not None:
                try:
                    float(data[field])
                except (ValueError, TypeError):
                    return False, f"Campo {field} deve ser um número válido"

        return True, None
    except Exception as e:
        return False, str(e)

def convert_radar_data(data):
    """Converte dados do formato do radar para o formato do banco"""
    try:
        # Extrair coordenadas do primeiro alvo (se existir)
        targets = data.get('targets', [])
        if not targets:
            return None, "Nenhum alvo detectado"
        
        first_target = targets[0]
        
        # Calcular move_speed baseado na distância
        # Se a distância mudou, consideramos que houve movimento
        distance = float(data.get('distance', 0))
        move_speed = 0 if distance == 0 else 1
        
        converted_data = {
            'x_point': float(first_target.get('x', 0)),
            'y_point': float(first_target.get('y', 0)),
            'move_speed': move_speed,
            'heart_rate': float(data.get('heart', 0)),
            'breath_rate': float(data.get('breath', 0)),
            'device_id': data.get('device_id', 'UNKNOWN')
        }
        
        return converted_data, None
    except Exception as e:
        return None, f"Erro ao converter dados: {str(e)}"

@app.route('/radar/data', methods=['POST'])
def receive_radar_data():
    """Endpoint para receber dados do radar"""
    try:
        logger.info("📡 Requisição POST recebida em /radar/data")
        logger.debug(f"Headers: {dict(request.headers)}")
        
        # Verificar Content-Type
        if not request.is_json:
            logger.error("❌ Content-Type não é application/json")
            logger.debug(f"Content-Type recebido: {request.content_type}")
            return jsonify({
                "status": "error",
                "message": "Content-Type deve ser application/json"
            }), 400

        # Tentar parsear o JSON
        try:
            raw_data = request.get_json()
            logger.debug(f"Dados brutos recebidos: {raw_data}")
            
            # Converter dados do formato do radar
            converted_data, error = convert_radar_data(raw_data)
            if error:
                logger.error(f"❌ Erro na conversão dos dados: {error}")
                return jsonify({
                    "status": "error",
                    "message": error
                }), 400
            
            logger.debug(f"Dados convertidos: {converted_data}")
            
        except Exception as e:
            logger.error(f"❌ Erro ao parsear JSON: {e}")
            return jsonify({
                "status": "error",
                "message": "JSON inválido"
            }), 400

        # Validar dados convertidos
        is_valid, error_message = validate_data(converted_data)
        if not is_valid:
            logger.error(f"❌ Dados inválidos: {error_message}")
            return jsonify({
                "status": "error",
                "message": error_message
            }), 400

        # Verificar DatabaseManager
        if db_manager is None:
            logger.error("❌ DatabaseManager não está disponível")
            return jsonify({
                "status": "error",
                "message": "Erro interno do servidor: Banco de dados não disponível"
            }), 500
        
        # Salvar no banco
        db_manager.insert_data(converted_data)
        
        return jsonify({
            "status": "success",
            "message": "Dados processados com sucesso",
            "processed_data": converted_data
        })

    except Exception as e:
        logger.error(f"❌ Erro ao processar dados: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno do servidor: {str(e)}"
        }), 500

@app.route('/radar/status', methods=['GET'])
def get_status():
    """Endpoint para verificar status do servidor e últimos dados"""
    try:
        status = {
            "server": "online",
            "database": "offline",
            "last_records": None,
            "engagement_stats": None
        }

        if db_manager and db_manager.conn.is_connected():
            status["database"] = "online"
            status["last_records"] = db_manager.get_last_records(5)
            status["engagement_stats"] = db_manager.get_engagement_stats()

        return jsonify(status)
    except Exception as e:
        logger.error(f"Erro ao verificar status: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/radar/engagement', methods=['GET'])
def get_engagement():
    """Endpoint para obter estatísticas de engajamento"""
    try:
        if not db_manager:
            return jsonify({
                "status": "error",
                "message": "Banco de dados não disponível"
            }), 500

        stats = db_manager.get_engagement_stats()
        return jsonify({
            "status": "success",
            "data": stats
        })
    except Exception as e:
        logger.error(f"Erro ao buscar engajamento: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    print("\n" + "="*50)
    print("🚀 Servidor Radar iniciando...")
    print(f"📡 Endpoint dados: http://{host}:{port}/radar/data")
    print(f"ℹ️  Endpoint status: http://{host}:{port}/radar/status")
    print(f"📊 Endpoint engajamento: http://{host}:{port}/radar/engagement")
    print("⚡ Use Ctrl+C para encerrar")
    print("="*50 + "\n")
    
    app.run(host=host, port=port, debug=True) 
