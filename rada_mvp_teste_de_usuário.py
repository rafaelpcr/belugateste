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
import uuid

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
                        session_id VARCHAR(36),
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
                    'engagement_duration': 'ADD COLUMN engagement_duration INT',
                    'session_id': 'ADD COLUMN session_id VARCHAR(36)'
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
            
            # Criar tabela para resumo de sessões
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS radar_sessoes (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    session_id VARCHAR(36) UNIQUE,
                    start_time DATETIME,
                    end_time DATETIME,
                    duration FLOAT,
                    avg_heart_rate FLOAT,
                    avg_breath_rate FLOAT,
                    avg_satisfaction FLOAT,
                    satisfaction_class VARCHAR(20),
                    is_engaged BOOLEAN,
                    data_points INT
                )
            """)
            logger.info("Tabela radar_sessoes criada/verificada com sucesso!")
            
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
            
            # Verificar se o dado tem o campo is_engaged (prioridade sobre analytics)
            if 'is_engaged' in data and data['is_engaged'] is not None:
                is_engaged = bool(data['is_engaged'])
                logger.info(f"Campo is_engaged encontrado nos dados: {is_engaged}")
            
            # Preparar query com campos adicionais
            query = """
                INSERT INTO radar_dados
                (x_point, y_point, move_speed, heart_rate, breath_rate, 
                 satisfaction_score, satisfaction_class, is_engaged, engagement_duration, session_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                engagement_duration,
                data.get('session_id')
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

    def save_session_summary(self, session_data):
        """Salva o resumo da sessão no banco de dados"""
        try:
            logger.info("="*50)
            logger.info(f"Salvando resumo da sessão {session_data['session_id']}...")
            
            # Verificar conexão antes de inserir
            if not self.conn or not self.conn.is_connected():
                logger.info("Conexão não disponível, tentando reconectar...")
                self.connect_with_retry()
            
            # Preparar query
            query = """
                INSERT INTO radar_sessoes
                (session_id, start_time, end_time, duration, avg_heart_rate, 
                 avg_breath_rate, avg_satisfaction, satisfaction_class, is_engaged, data_points)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                end_time = VALUES(end_time),
                duration = VALUES(duration),
                avg_heart_rate = VALUES(avg_heart_rate),
                avg_breath_rate = VALUES(avg_breath_rate),
                avg_satisfaction = VALUES(avg_satisfaction),
                satisfaction_class = VALUES(satisfaction_class),
                is_engaged = VALUES(is_engaged),
                data_points = VALUES(data_points)
            """
            
            # Determinar classificação de satisfação
            satisfaction_class = "NEUTRA"
            if session_data.get('avg_satisfaction') is not None:
                if session_data['avg_satisfaction'] >= 70:
                    satisfaction_class = "POSITIVA"
                elif session_data['avg_satisfaction'] <= 40:
                    satisfaction_class = "NEGATIVA"
            
            # Preparar parâmetros
            start_time = session_data.get('start_time')
            end_time = session_data.get('end_time')
            
            # Converter para string se for datetime
            if isinstance(start_time, datetime):
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(end_time, datetime):
                end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
            
            params = (
                session_data.get('session_id'),
                start_time,
                end_time,
                float(session_data.get('duration', 0)),
                float(session_data.get('avg_heart_rate', 0)) if session_data.get('avg_heart_rate') is not None else None,
                float(session_data.get('avg_breath_rate', 0)) if session_data.get('avg_breath_rate') is not None else None,
                float(session_data.get('avg_satisfaction', 0)) if session_data.get('avg_satisfaction') is not None else None,
                satisfaction_class,
                bool(session_data.get('is_engaged', False)),
                len(session_data.get('positions', []))
            )
            
            logger.info(f"Query SQL: {query}")
            logger.info(f"Parâmetros: {params}")
            
            # Executar inserção
            self.cursor.execute(query, params)
            self.conn.commit()
            
            logger.info(f"✅ Resumo da sessão {session_data['session_id']} salvo com sucesso!")
            logger.info("="*50)
            return True
            
        except Exception as e:
            logger.error("="*50)
            logger.error(f"❌ Erro ao salvar resumo da sessão: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            logger.error(f"Dados da sessão: {session_data}")
            logger.error("="*50)
            raise

    def get_sessions(self, limit=10):
        """Obtém as sessões mais recentes"""
        try:
            if not self.conn or not self.conn.is_connected():
                self.connect_with_retry()
                
            query = """
                SELECT * FROM radar_sessoes
                ORDER BY end_time DESC 
                LIMIT %s
            """
            
            self.cursor.execute(query, (limit,))
            sessions = self.cursor.fetchall()
            
            # Converter datetime para string
            for session in sessions:
                if isinstance(session['start_time'], datetime):
                    session['start_time'] = session['start_time'].strftime('%Y-%m-%d %H:%M:%S')
                if isinstance(session['end_time'], datetime):
                    session['end_time'] = session['end_time'].strftime('%Y-%m-%d %H:%M:%S')
            
            return sessions
        except Exception as e:
            logger.error(f"Erro ao buscar sessões: {str(e)}")
            logger.error(traceback.format_exc())
            return []
            
    def get_session_by_id(self, session_id):
        """Obtém uma sessão específica pelo ID"""
        try:
            if not self.conn or not self.conn.is_connected():
                self.connect_with_retry()
                
            # Buscar resumo da sessão
            query_session = """
                SELECT * FROM radar_sessoes
                WHERE session_id = %s
            """
            
            self.cursor.execute(query_session, (session_id,))
            session = self.cursor.fetchone()
            
            if not session:
                return None
                
            # Converter datetime para string
            if isinstance(session['start_time'], datetime):
                session['start_time'] = session['start_time'].strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(session['end_time'], datetime):
                session['end_time'] = session['end_time'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Buscar pontos de dados da sessão
            query_points = """
                SELECT * FROM radar_dados
                WHERE session_id = %s
                ORDER BY timestamp ASC
            """
            
            self.cursor.execute(query_points, (session_id,))
            points = self.cursor.fetchall()
            
            # Converter datetime para string nos pontos
            for point in points:
                if isinstance(point['timestamp'], datetime):
                    point['timestamp'] = point['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Adicionar pontos à sessão
            session['data_points'] = points
            
            return session
        except Exception as e:
            logger.error(f"Erro ao buscar sessão {session_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

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
        self.ENGAGEMENT_TIME_THRESHOLD = 3  # segundos (reduzido de 5 para 3)
        self.MOVEMENT_THRESHOLD = 20.0  # limite para considerar "parado" em cm/s (era 0.2 m/s, agora 20 cm/s)
        
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
        Retorna True se pessoa ficou parada por mais de 3 segundos
        move_speed está em cm/s
        """
        if not records or len(records) < 2:
            logger.info("Não há registros suficientes para calcular engajamento")
            return False
            
        # Filtrar registros válidos
        valid_records = []
        for record in records:
            if (record.get('move_speed') is not None):
                valid_records.append(record)
        
        if len(valid_records) < 2:
            logger.info("Não há registros válidos suficientes para calcular engajamento")
            return False
            
        # Verificar se há registros recentes com movimento baixo
        paused_records = 0
        for record in valid_records:
            try:
                move_speed = float(record.get('move_speed', 999))
                logger.info(f"Verificando engajamento: move_speed = {move_speed} cm/s")
                
                if move_speed <= self.MOVEMENT_THRESHOLD:
                    paused_records += 1
                    logger.info(f"Registro com movimento baixo detectado: {move_speed} cm/s <= {self.MOVEMENT_THRESHOLD} cm/s")
                    
                    # Se tivermos pelo menos 3 registros com movimento baixo, consideramos engajado
                    if paused_records >= 2:
                        logger.info(f"Engajamento detectado! {paused_records} registros com movimento baixo")
                        return True
            except Exception as e:
                logger.error(f"Erro ao processar registro para engajamento: {str(e)}")
                continue
                
        logger.info(f"Engajamento não detectado. Apenas {paused_records} registros com movimento baixo")
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

class UserSessionManager:
    def __init__(self):
        # Constantes para detecção de entrada/saída
        self.PRESENCE_THRESHOLD = 2.0  # Distância máxima para considerar presença (metros)
        self.MOVEMENT_THRESHOLD = 50.0  # Movimento máximo para considerar "parado" em cm/s (era 0.5 m/s, agora 50 cm/s)
        self.ABSENCE_THRESHOLD = 3.0   # Distância mínima para considerar ausência (metros)
        self.TIME_THRESHOLD = 2        # Tempo mínimo (segundos) para considerar uma nova sessão
        
        # Estado atual
        self.current_session_id = None
        self.last_presence_time = None
        self.last_absence_time = None
        self.session_start_time = None
        self.session_data = {}
        self.last_position = (None, None)
        self.last_distance = None
        self.is_present = False
        self.consecutive_presence_count = 0  # Contador de presenças consecutivas
        
    def detect_session(self, data, timestamp=None):
        """
        Detecta se uma pessoa entrou ou saiu da área da gôndola
        Retorna: (session_id, event_type, session_data)
        event_type pode ser: 'start', 'update', 'end', None
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        # Extrair dados relevantes
        x_point = data.get('x_point')
        y_point = data.get('y_point')
        move_speed = data.get('move_speed', 999)
        
        # Calcular distância do centro (0,0)
        distance = np.sqrt(x_point**2 + y_point**2) if x_point is not None and y_point is not None else None
        
        # Log para debug
        logger.info(f"Detecção de sessão: x={x_point}, y={y_point}, move_speed={move_speed} cm/s, distância={distance} m")
        
        # Inicializar evento como None (sem evento)
        event_type = None
        
        # Verificar se há dados suficientes
        if distance is None:
            return self.current_session_id, None, None
            
        # Detectar presença/ausência
        was_present = self.is_present
        
        # Pessoa está presente se estiver próxima e com movimento limitado
        if distance <= self.PRESENCE_THRESHOLD:
            self.consecutive_presence_count += 1
            logger.info(f"Presença detectada! Contagem: {self.consecutive_presence_count}")
            
            # Só consideramos presente após 2 detecções consecutivas
            if self.consecutive_presence_count >= 2:
                self.is_present = True
                self.last_presence_time = timestamp
                
                # Se não havia sessão, iniciar uma nova
                if self.current_session_id is None:
                    self.current_session_id = str(uuid.uuid4())
                    self.session_start_time = timestamp
                    self.session_data = {
                        'session_id': self.current_session_id,
                        'start_time': timestamp,
                        'heart_rates': [],
                        'breath_rates': [],
                        'positions': [],
                        'move_speeds': [],
                        'satisfaction_scores': [],
                        'is_engaged': False
                    }
                    event_type = 'start'
                    logger.info(f"🟢 Nova sessão iniciada: {self.current_session_id}")
                else:
                    event_type = 'update'
                    
                # Atualizar dados da sessão
                if data.get('heart_rate') is not None:
                    self.session_data['heart_rates'].append(data.get('heart_rate'))
                if data.get('breath_rate') is not None:
                    self.session_data['breath_rates'].append(data.get('breath_rate'))
                if data.get('satisfaction_score') is not None:
                    self.session_data['satisfaction_scores'].append(data.get('satisfaction_score'))
                
                self.session_data['positions'].append((x_point, y_point))
                self.session_data['move_speeds'].append(move_speed)
                
                # Verificar engajamento baseado no movimento
                if move_speed <= self.MOVEMENT_THRESHOLD:
                    logger.info(f"Movimento baixo detectado: {move_speed} cm/s <= {self.MOVEMENT_THRESHOLD} cm/s")
                    self.session_data['is_engaged'] = True
        else:
            self.consecutive_presence_count = 0
            
            # Pessoa está ausente se estiver longe
            if distance >= self.ABSENCE_THRESHOLD:
                self.is_present = False
                self.last_absence_time = timestamp
                
                # Se havia uma sessão ativa, finalizá-la
                if was_present and self.current_session_id is not None:
                    # Calcular métricas finais da sessão
                    session_duration = (timestamp - self.session_start_time).total_seconds()
                    
                    # Só considerar sessão válida se durou mais que o tempo mínimo
                    if session_duration >= self.TIME_THRESHOLD:
                        # Calcular médias
                        avg_heart_rate = np.mean(self.session_data['heart_rates']) if self.session_data['heart_rates'] else None
                        avg_breath_rate = np.mean(self.session_data['breath_rates']) if self.session_data['breath_rates'] else None
                        avg_satisfaction = np.mean(self.session_data['satisfaction_scores']) if self.session_data['satisfaction_scores'] else None
                        
                        # Adicionar métricas finais
                        self.session_data['end_time'] = timestamp
                        self.session_data['duration'] = session_duration
                        self.session_data['avg_heart_rate'] = avg_heart_rate
                        self.session_data['avg_breath_rate'] = avg_breath_rate
                        self.session_data['avg_satisfaction'] = avg_satisfaction
                        
                        event_type = 'end'
                        logger.info(f"🔴 Sessão finalizada: {self.current_session_id}, duração: {session_duration:.2f}s")
                        
                        # Guardar dados da sessão antes de resetar
                        session_data_copy = self.session_data.copy()
                        
                        # Resetar sessão
                        self.current_session_id = None
                        self.session_start_time = None
                        self.session_data = {}
                        
                        return session_data_copy['session_id'], event_type, session_data_copy
        
        # Atualizar última posição e distância
        self.last_position = (x_point, y_point)
        self.last_distance = distance
        
        return self.current_session_id, event_type, self.session_data

# Instância global do gerenciador de sessões
user_session_manager = UserSessionManager()

@app.route('/radar/data', methods=['POST'])
def receive_radar_data():
    """Endpoint para receber dados do radar"""
    try:
        logger.info("="*50)
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
            # Obter últimos registros para análise
            last_records = db_manager.get_last_records(10)
            
            # Adicionar o registro atual aos últimos registros para análise
            current_record = {
                'x_point': converted_data.get('x_point'),
                'y_point': converted_data.get('y_point'),
                'move_speed': converted_data.get('move_speed'),
                'heart_rate': converted_data.get('heart_rate'),
                'breath_rate': converted_data.get('breath_rate'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Adicionar o registro atual à lista para análise
            analysis_records = [current_record] + last_records if last_records else [current_record]
            
            # Calcular engajamento com os registros combinados
            is_engaged = analytics_manager.calculate_engagement(analysis_records)
            analytics_data["engaged"] = is_engaged
            
            logger.info(f"Resultado do cálculo de engajamento: {is_engaged}")
            
            # Se o movimento for baixo, considerar engajado diretamente
            if converted_data.get('move_speed') is not None and float(converted_data.get('move_speed')) <= analytics_manager.MOVEMENT_THRESHOLD:
                logger.info(f"Engajamento direto detectado! move_speed = {converted_data.get('move_speed')} cm/s <= {analytics_manager.MOVEMENT_THRESHOLD} cm/s")
                analytics_data["engaged"] = True
                
        except Exception as e:
            logger.error(f"❌ Erro ao calcular engajamento: {str(e)}")
            logger.error(traceback.format_exc())
        
        # Adicionar dados de satisfação ao converted_data
        converted_data['satisfaction_score'] = analytics_data['satisfaction']['score']
        converted_data['satisfaction_class'] = analytics_data['satisfaction']['classification']
        converted_data['is_engaged'] = analytics_data['engaged']
        
        # Detectar sessão de usuário
        try:
            session_id, event_type, session_data = user_session_manager.detect_session(converted_data)
            
            # Adicionar ID da sessão aos dados
            converted_data['session_id'] = session_id
            
            # Se a sessão terminou, salvar resumo da sessão
            if event_type == 'end' and session_data:
                logger.info(f"📊 Resumo da sessão {session_id}:")
                logger.info(f"   Duração: {session_data['duration']:.2f} segundos")
                logger.info(f"   Satisfação média: {session_data.get('avg_satisfaction')}")
                logger.info(f"   Engajado: {session_data.get('is_engaged')}")
                
                # Salvar resumo da sessão no banco
                try:
                    db_manager.save_session_summary(session_data)
                except Exception as e:
                    logger.error(f"❌ Erro ao salvar resumo da sessão: {str(e)}")
                    logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(f"❌ Erro ao detectar sessão: {str(e)}")
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
            "analytics": analytics_data,
            "session": {
                "id": session_id,
                "event": event_type
            }
        }
        
        logger.info("="*50)
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

@app.route('/radar/sessions', methods=['GET'])
def get_sessions():
    """Endpoint para listar sessões"""
    try:
        if not db_manager:
            return jsonify({
                "status": "error",
                "message": "Banco de dados não disponível"
            }), 500
            
        # Obter limite da query string
        limit = request.args.get('limit', default=10, type=int)
        
        # Obter sessões
        sessions = db_manager.get_sessions(limit)
        
        return jsonify({
            "status": "success",
            "count": len(sessions),
            "sessions": sessions
        })
    except Exception as e:
        logger.error(f"Erro ao listar sessões: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno: {str(e)}"
        }), 500

@app.route('/radar/sessions/<session_id>', methods=['GET'])
def get_session(session_id):
    """Endpoint para obter detalhes de uma sessão"""
    try:
        if not db_manager:
            return jsonify({
                "status": "error",
                "message": "Banco de dados não disponível"
            }), 500
            
        # Obter sessão
        session = db_manager.get_session_by_id(session_id)
        
        if not session:
            return jsonify({
                "status": "error",
                "message": f"Sessão {session_id} não encontrada"
            }), 404
        
        return jsonify({
            "status": "success",
            "session": session
        })
    except Exception as e:
        logger.error(f"Erro ao obter sessão {session_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno: {str(e)}"
        }), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3000))
    host = os.getenv("HOST", "0.0.0.0")
    
    print("\n" + "="*50)
    print("🚀 Servidor Radar iniciando...")
    print(f"📡 Endpoint dados: http://{host}:{port}/radar/data")
    print(f"ℹ️  Endpoint status: http://{host}:{port}/radar/status")
    print(f"👥 Endpoint sessões: http://{host}:{port}/radar/sessions")
    print(f"👤 Endpoint sessão específica: http://{host}:{port}/radar/sessions/<session_id>")
    print("⚡ Use Ctrl+C para encerrar")
    print("="*50 + "\n")
    
    app.run(host=host, port=port, debug=True) 
