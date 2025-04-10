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
    """Converte dados brutos do radar para o formato do banco de dados"""
    try:
        # Extrair dados do JSON
        data = json.loads(raw_data)
        
        # Converter valores para float
        x_point = float(data.get('x_point', 0))
        y_point = float(data.get('y_point', 0))
        move_speed = float(data.get('move_speed', 0))
        heart_rate = float(data.get('heart_rate', 0))
        breath_rate = float(data.get('breath_rate', 0))
        
        # Calcular satisfação
        satisfaction_score = calculate_satisfaction(move_speed, heart_rate, breath_rate)
        satisfaction_class = classify_satisfaction(satisfaction_score)
        
        # Identificar se está engajado
        is_engaged = move_speed < 0.5  # Considera engajado se velocidade < 0.5 m/s
        
        # Identificar seção e produto
        section = shelf_manager.get_section_at_position(x_point, y_point)
        section_id = section['id'] if section else None
        product_id = section['product_id'] if section else None
        
        return {
            'x_point': x_point,
            'y_point': y_point,
            'move_speed': move_speed,
            'heart_rate': heart_rate,
            'breath_rate': breath_rate,
            'satisfaction_score': satisfaction_score,
            'satisfaction_class': satisfaction_class,
            'is_engaged': is_engaged,
            'section_id': section_id,
            'product_id': product_id
        }
    except Exception as e:
        logger.error(f"Erro ao converter dados do radar: {str(e)}")
        return None

class ShelfManager:
    def __init__(self):
        # Constantes para mapeamento de seções
        self.SECTION_WIDTH = 0.5  # Largura de cada seção em metros
        self.SECTION_HEIGHT = 0.3  # Altura de cada seção em metros
        self.MAX_SECTIONS_X = 4    # Número máximo de seções na horizontal
        self.MAX_SECTIONS_Y = 3    # Número máximo de seções na vertical
        
    def initialize_database(self, db_manager):
        """Inicializa a tabela de seções da gôndola"""
        try:
            # Criar tabela para seções da gôndola
            db_manager.cursor.execute("""
                CREATE TABLE IF NOT EXISTS shelf_sections (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    section_name VARCHAR(50),
                    x_start FLOAT,
                    y_start FLOAT,
                    x_end FLOAT,
                    y_end FLOAT,
                    product_id VARCHAR(50),
                    product_name VARCHAR(100),
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)
            db_manager.conn.commit()
            logger.info("✅ Tabela shelf_sections criada/verificada com sucesso!")
            
        except Exception as e:
            logger.error(f"❌ Erro ao inicializar tabela shelf_sections: {str(e)}")
            logger.error(traceback.format_exc())
            raise
            
    def get_section_at_position(self, x, y):
        """
        Identifica a seção da gôndola baseado nas coordenadas (x, y)
        Retorna: dict com informações da seção ou None se não encontrar
        """
        try:
            # Adicionar margem de tolerância para detecção da seção
            MARGIN = 0.05  # 5cm de margem
            
            # Buscar seções ativas que contenham o ponto (x, y) com margem de tolerância
            query = """
                SELECT id as section_id, section_name as name, product_id,
                       x_start, x_end, y_start, y_end
                FROM shelf_sections
                WHERE is_active = TRUE
                AND x_start - %s <= %s AND x_end + %s >= %s
                AND y_start - %s <= %s AND y_end + %s >= %s
                ORDER BY ABS(x_start - %s) + ABS(y_start - %s)
                LIMIT 1
            """
            
            params = (MARGIN, x, MARGIN, x, MARGIN, y, MARGIN, y, x, y)
            
            db_manager.cursor.execute(query, params)
            section = db_manager.cursor.fetchone()
            
            if section:
                # Calcular distância do ponto ao centro da seção
                center_x = (section['x_start'] + section['x_end']) / 2
                center_y = (section['y_start'] + section['y_end']) / 2
                distance = ((x - center_x) ** 2 + (y - center_y) ** 2) ** 0.5
                
                logger.info(f"✅ Seção encontrada: {section['name']} (Produto: {section['product_id']})")
                logger.info(f"   Distância ao centro: {distance:.2f}m")
                logger.info(f"   Coordenadas da seção: x=[{section['x_start']:.2f}, {section['x_end']:.2f}], y=[{section['y_start']:.2f}, {section['y_end']:.2f}]")
                return section
            else:
                logger.info(f"❌ Nenhuma seção encontrada para as coordenadas (x={x}, y={y})")
                return None
                
        except Exception as e:
            logger.error(f"❌ Erro ao buscar seção: {str(e)}")
            logger.error(traceback.format_exc())
            return None
            
    def add_section(self, section_data, db_manager):
        """
        Adiciona uma nova seção à gôndola
        section_data: dict com section_name, x_start, y_start, x_end, y_end, product_id, product_name
        """
        try:
            query = """
                INSERT INTO shelf_sections
                (section_name, x_start, y_start, x_end, y_end, product_id, product_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (
                section_data['section_name'],
                section_data['x_start'],
                section_data['y_start'],
                section_data['x_end'],
                section_data['y_end'],
                section_data['product_id'],
                section_data['product_name']
            )
            
            db_manager.cursor.execute(query, params)
            db_manager.conn.commit()
            
            logger.info(f"✅ Seção {section_data['section_name']} adicionada com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao adicionar seção: {str(e)}")
            logger.error(traceback.format_exc())
            return False
            
    def update_section(self, section_id, section_data, db_manager):
        """
        Atualiza uma seção existente
        section_data: dict com os campos a serem atualizados
        """
        try:
            # Construir query dinamicamente baseado nos campos fornecidos
            update_fields = []
            params = []
            
            for field, value in section_data.items():
                if field in ['section_name', 'x_start', 'y_start', 'x_end', 'y_end', 
                           'product_id', 'product_name', 'is_active']:
                    update_fields.append(f"{field} = %s")
                    params.append(value)
            
            if not update_fields:
                logger.warning("Nenhum campo para atualizar")
                return False
                
            # Adicionar section_id aos parâmetros
            params.append(section_id)
            
            query = f"""
                UPDATE shelf_sections
                SET {', '.join(update_fields)}
                WHERE id = %s
            """
            
            db_manager.cursor.execute(query, params)
            db_manager.conn.commit()
            
            logger.info(f"✅ Seção {section_id} atualizada com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar seção: {str(e)}")
            logger.error(traceback.format_exc())
            return False
            
    def get_all_sections(self, db_manager):
        """Retorna todas as seções ativas"""
        try:
            query = """
                SELECT * FROM shelf_sections
                WHERE is_active = TRUE
                ORDER BY section_name
            """
            
            db_manager.cursor.execute(query)
            sections = db_manager.cursor.fetchall()
            
            return sections
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar seções: {str(e)}")
            logger.error(traceback.format_exc())
            return []

# Instância global do gerenciador de seções
shelf_manager = ShelfManager()

# Configurações do MySQL
db_config = {
    "host": os.getenv("DB_HOST", "168.75.89.11"),
    "user": os.getenv("DB_USER", "belugaDB"),
    "password": os.getenv("DB_PASSWORD", "Rpcr@300476"),
    "database": os.getenv("DB_NAME", "Beluga_Analytics"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "use_pure": True,
    "ssl_disabled": True
}

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
                
                self.conn = mysql.connector.connect(**db_config)
                self.cursor = self.conn.cursor(dictionary=True, buffered=True)
                
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
            # Dropar tabela areas existente
            logger.info("Removendo tabela areas antiga...")
            self.cursor.execute("DROP TABLE IF EXISTS areas")
            
            # Criar nova tabela areas
            logger.info("Criando nova tabela areas...")
            self.cursor.execute("""
                CREATE TABLE areas (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    area_name VARCHAR(50) NOT NULL,
                    y_min FLOAT NOT NULL,
                    y_max FLOAT NOT NULL,
                    speed_threshold FLOAT NOT NULL,
                    description TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)
            
            # Inserir áreas padrão
            logger.info("Adicionando areas padrão...")
            self.cursor.execute("""
                INSERT INTO areas 
                (area_name, y_min, y_max, speed_threshold, description)
                VALUES 
                ('PASSAGEM', 0.5, 999999.0, 0.5, 'Cliente apenas passando'),
                ('ATENCAO', 0.3, 0.5, 0.3, 'Cliente olhando de longe'),
                ('CONSIDERACAO', 0.15, 0.3, 0.2, 'Cliente analisando produtos'),
                ('INTERACAO', 0.0, 0.15, 0.1, 'Cliente próximo, possivelmente pegando produto')
            """)
            logger.info("✅ Áreas padrão criadas com sucesso")
            
            # Verificar tabela de dispositivos
            logger.info("Verificando tabela de dispositivos...")
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS Dispositivos (
                    serial_number VARCHAR(50) PRIMARY KEY,
                    nome VARCHAR(100),
                    tipo VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'ATIVO',
                    data_cadastro DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Verificar dispositivo padrão
            logger.info("Verificando dispositivo padrão...")
            self.cursor.execute("""
                INSERT IGNORE INTO Dispositivos 
                (serial_number, nome, tipo)
                VALUES 
                ('RADAR_1', 'Radar Gôndola Principal', 'RADAR')
            """)
            
            # Verificar tabela radar_dados
            logger.info("Verificando tabela radar_dados...")
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS radar_dados (
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
                    section_id INT,
                    product_id VARCHAR(20),
                    timestamp DATETIME,
                    serial_number VARCHAR(20)
                )
            """)
            self.conn.commit()
            logger.info("✅ Tabela radar_dados criada/verificada com sucesso!")
            
            # Verificar tabela radar_sessoes
            logger.info("Verificando tabela radar_sessoes...")
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS radar_sessoes (
                    session_id VARCHAR(50) PRIMARY KEY,
                    start_time DATETIME,
                    end_time DATETIME,
                    duration INT,
                    avg_heart_rate FLOAT,
                    avg_breath_rate FLOAT,
                    avg_satisfaction FLOAT,
                    satisfaction_class VARCHAR(20),
                    is_engaged BOOLEAN,
                    data_points INT
                )
            """)
            
            # Verificar tabela shelf_sections
            logger.info("Verificando tabela shelf_sections...")
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS shelf_sections (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    section_name VARCHAR(50),
                    x_start FLOAT,
                    y_start FLOAT,
                    x_end FLOAT,
                    y_end FLOAT,
                    product_id VARCHAR(50),
                    product_name VARCHAR(100),
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)
            
            # Verificar se já existem seções
            self.cursor.execute("SELECT COUNT(*) as count FROM shelf_sections")
            count = self.cursor.fetchone()['count']
            
            # Só adicionar seções padrão se a tabela estiver vazia
            if count == 0:
                logger.info("Adicionando seções padrão...")
                self.cursor.execute("""
                    INSERT INTO shelf_sections 
                    (section_name, x_start, y_start, x_end, y_end, product_id, product_name)
                    VALUES 
                    ('Granolas Premium', -0.75, 0.0, -0.25, 0.3, 'GRN001', 'Granolas Premium'),
                    ('Mix de Frutas Secas', -0.25, 0.0, 0.25, 0.3, 'MIX001', 'Mix de Frutas Secas'),
                    ('Barras de Cereais', 0.25, 0.0, 0.75, 0.3, 'BAR001', 'Barras de Cereais')
                """)

            self.conn.commit()
            logger.info("✅ Banco de dados atualizado com sucesso!")
            
        except Exception as e:
            logger.error(f"❌ Erro ao inicializar banco: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def ensure_device_exists(self, serial_number, nome=None, tipo=None):
        """Garante que o dispositivo existe no banco"""
        try:
            # Verificar se o dispositivo já existe
            self.cursor.execute("""
                SELECT serial_number FROM Dispositivos
                WHERE serial_number = %s
            """, (serial_number,))
            
            device = self.cursor.fetchone()
            
            if not device:
                # Inserir novo dispositivo
                logger.info(f"Inserindo novo dispositivo: {serial_number}")
                self.cursor.execute("""
                    INSERT INTO Dispositivos (serial_number, nome, tipo)
                    VALUES (%s, %s, %s)
                """, (
                    serial_number,
                    nome or f"Radar {serial_number}",
                    tipo or "RADAR"
                ))
                self.conn.commit()
                logger.info(f"✅ Dispositivo {serial_number} inserido com sucesso!")
            
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao verificar/inserir dispositivo: {str(e)}")
            return False

    def insert_data(self, data, analytics_data=None):
        """Insere dados no banco"""
        max_retries = 3
        retry_delay = 1  # segundos
        
        for attempt in range(max_retries):
            try:
                logger.info("="*50)
                logger.info("Iniciando inserção de dados no banco...")
                
                # Verificar conexão antes de inserir
                if not self.conn or not self.conn.is_connected():
                    logger.info("Conexão não disponível, tentando reconectar...")
                    self.connect_with_retry()
                
                # Garantir que o dispositivo existe
                serial_number = data.get('serial_number', 'RADAR_1')
                if not self.ensure_device_exists(serial_number):
                    return False
                
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
                
                # Inserir dados com transaction
                self.cursor.execute("START TRANSACTION")
                
                query = """
                    INSERT INTO radar_dados
                    (x_point, y_point, move_speed, heart_rate, breath_rate,
                    satisfaction_score, satisfaction_class, is_engaged, engagement_duration,
                    session_id, section_id, product_id, serial_number, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    data['x_point'], data['y_point'], data['move_speed'],
                    data['heart_rate'], data['breath_rate'],
                    satisfaction_score, satisfaction_class, is_engaged, engagement_duration,
                    data.get('session_id'), data.get('section_id'), data.get('product_id'),
                    serial_number, datetime.now()
                )
                
                self.cursor.execute(query, values)
                self.conn.commit()
                
                logger.info("✅ Dados inseridos com sucesso!")
                return True
                
            except mysql.connector.Error as err:
                if err.errno == 1205:  # Lock timeout error
                    if attempt < max_retries - 1:
                        logger.warning(f"Lock timeout, tentativa {attempt + 1} de {max_retries}")
                        time.sleep(retry_delay)
                        continue
                logger.error(f"❌ Erro MySQL ao inserir dados: {err}")
                try:
                    self.conn.rollback()
                except:
                    pass
                return False
                
            except Exception as e:
                logger.error(f"❌ Erro ao inserir dados: {e}")
                try:
                    self.conn.rollback()
                except:
                    pass
                return False
                
        return False

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

    def get_active_session(self, x_point, y_point, move_speed, timestamp):
        """Verifica se existe uma sessão ativa para as coordenadas fornecidas"""
        try:
            # Buscar a última sessão registrada nos últimos 5 minutos
            query = """
                SELECT DISTINCT session_id, timestamp
                FROM radar_dados
                WHERE timestamp >= DATE_SUB(%s, INTERVAL 5 MINUTE)
                AND ABS(x_point - %s) < 0.5
                AND ABS(y_point - %s) < 0.5
                ORDER BY timestamp DESC
                LIMIT 1
            """
            
            self.cursor.execute(query, (timestamp, x_point, y_point))
            result = self.cursor.fetchone()
            
            if result:
                logger.info(f"Sessão ativa encontrada: {result['session_id']}")
                return result['session_id']
            
            return None
            
        except Exception as e:
            logger.error(f"Erro ao buscar sessão ativa: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def insert_radar_data(self, data):
        """Insere dados do radar no banco"""
        max_retries = 3
        retry_delay = 2  # segundos
        
        for attempt in range(max_retries):
            try:
                logger.info("="*50)
                logger.info("Iniciando inserção de dados no banco...")
                logger.info(f"Dados recebidos: {data}")
                
                # Verificar campos obrigatórios
                required_fields = ['x_point', 'y_point', 'move_speed']
                for field in required_fields:
                    if field not in data:
                        logger.error(f"Campo obrigatório ausente: {field}")
                        return False
                
                # Garantir valores padrão para campos que podem estar ausentes
                if 'satisfaction_score' not in data or data['satisfaction_score'] is None:
                    data['satisfaction_score'] = 0.0
                    
                if 'satisfaction_class' not in data or data['satisfaction_class'] is None:
                    data['satisfaction_class'] = 'NEUTRA'
                    
                if 'is_engaged' not in data or data['is_engaged'] is None:
                    data['is_engaged'] = False
                    
                if 'engagement_duration' not in data or data['engagement_duration'] is None:
                    data['engagement_duration'] = 0
                    
                if 'serial_number' not in data or data['serial_number'] is None:
                    data['serial_number'] = 'SERIAL_2'

                # Garantir que o dispositivo existe
                self.cursor.execute("""
                    INSERT IGNORE INTO Dispositivos 
                    (serial_number, nome, tipo)
                    VALUES 
                    (%s, %s, %s)
                """, (data['serial_number'], f"Radar {data['serial_number']}", 'RADAR'))
                self.conn.commit()
                    
                # Verificar se já existe uma sessão ativa
                timestamp = data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                active_session = self.get_active_session(
                    float(data.get('x_point')),
                    float(data.get('y_point')),
                    float(data.get('move_speed')),
                    timestamp
                )
                
                # Usar sessão existente ou criar nova
                if active_session:
                    data['session_id'] = active_session
                    logger.info(f"Usando sessão existente: {active_session}")
                elif 'session_id' not in data or data['session_id'] is None:
                    data['session_id'] = str(uuid.uuid4())
                    logger.info(f"Novo session_id gerado: {data['session_id']}")
                    
                if 'section_id' not in data or data['section_id'] is None:
                    data['section_id'] = 1
                    
                if 'product_id' not in data or data['product_id'] is None:
                    data['product_id'] = 'UNKNOWN'
                
                # Verificar conexão antes de inserir
                if not self.conn or not self.conn.is_connected():
                    logger.info("Reconectando ao banco de dados...")
                    self.connect_with_retry()
                
                # Query de inserção
                query = """
                    INSERT INTO radar_dados
                    (x_point, y_point, move_speed, heart_rate, breath_rate, 
                    satisfaction_score, satisfaction_class, is_engaged, engagement_duration, 
                    session_id, section_id, product_id, timestamp, serial_number)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # Preparar parâmetros
                params = (
                    float(data.get('x_point')),
                    float(data.get('y_point')),
                    float(data.get('move_speed')),
                    float(data.get('heart_rate')) if data.get('heart_rate') is not None else None,
                    float(data.get('breath_rate')) if data.get('breath_rate') is not None else None,
                    float(data.get('satisfaction_score', 0)),
                    data.get('satisfaction_class', 'NEUTRA'),
                    bool(data.get('is_engaged', False)),
                    int(data.get('engagement_duration', 0)),
                    data['session_id'],
                    int(data.get('section_id', 1)),
                    data.get('product_id', 'UNKNOWN'),
                    data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                    data.get('serial_number', 'RADAR_1')
                )
                
                logger.info(f"Query: {query}")
                logger.info(f"Parâmetros: {params}")
                
                # Executar inserção com retry em caso de deadlock
                try:
                    self.cursor.execute(query, params)
                    self.conn.commit()
                    logger.info("✅ Dados inseridos com sucesso!")
                    return True
                except mysql.connector.errors.DatabaseError as e:
                    if e.errno == 1205 and attempt < max_retries - 1:  # Lock timeout error
                        logger.warning(f"Lock timeout na tentativa {attempt + 1}, tentando novamente em {retry_delay} segundos...")
                        time.sleep(retry_delay)
                        continue
                    raise
                    
            except Exception as e:
                logger.error(f"❌ Erro ao inserir dados: {str(e)}")
                logger.error(traceback.format_exc())
                if attempt < max_retries - 1:
                    logger.info(f"Tentando novamente em {retry_delay} segundos...")
                    time.sleep(retry_delay)
                    continue
                return False
                
        return False

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
        self.MOVEMENT_THRESHOLD = 20.0  # limite para considerar "parado" em cm/s
        self.ENGAGEMENT_MIN_DURATION = 5  # duração mínima para considerar engajamento completo
        
        # Constantes para satisfação
        self.HEART_RATE_MIN = 60
        self.HEART_RATE_MAX = 100
        self.HEART_RATE_IDEAL = 75
        
        self.BREATH_RATE_MIN = 12
        self.BREATH_RATE_MAX = 20
        self.BREATH_RATE_IDEAL = 15
        
        # Pesos para o cálculo de satisfação
        self.WEIGHT_HEART_RATE = 0.5
        self.WEIGHT_RESP_RATE = 0.5
        
        # Rastreamento de engajamento
        self.engagement_start_time = None
        self.last_movement_time = None

    def calculate_satisfaction_score(self, move_speed, heart_rate, breath_rate):
        """
        Calcula o score de satisfação baseado nas métricas do radar
        Retorna: (score, classificação)
        """
        try:
            # Normalizar as métricas para uma escala de 0-1
            move_speed_norm = min(1.0, move_speed / 20.0)  # Velocidade máxima considerada: 20
            heart_rate_norm = max(0.0, min(1.0, (heart_rate - 60) / 40))  # Faixa normal: 60-100 bpm
            breath_rate_norm = max(0.0, min(1.0, (breath_rate - 12) / 8))  # Faixa normal: 12-20 rpm
            
            # Pesos para cada métrica
            WEIGHTS = {
                'move_speed': 0.5,    # Velocidade tem maior peso
                'heart_rate': 0.3,    # Frequência cardíaca tem peso médio
                'breath_rate': 0.2    # Respiração tem menor peso
            }
            
            # Calcular score ponderado (0-100)
            score = 100 * (
                WEIGHTS['move_speed'] * (1 - move_speed_norm) +  # Menor velocidade = maior satisfação
                WEIGHTS['heart_rate'] * (1 - heart_rate_norm) +  # Menor freq cardíaca = maior satisfação
                WEIGHTS['breath_rate'] * (1 - breath_rate_norm)  # Menor freq respiratória = maior satisfação
            )
            
            # Classificar o score
            if score >= 80:
                satisfaction_class = 'Muito Satisfeito'
            elif score >= 60:
                satisfaction_class = 'Satisfeito'
            elif score >= 40:
                satisfaction_class = 'Neutro'
            elif score >= 20:
                satisfaction_class = 'Insatisfeito'
            else:
                satisfaction_class = 'Muito Insatisfeito'
                
            logger.info(f"📊 Score de satisfação calculado:")
            logger.info(f"   Score: {score:.1f}/100")
            logger.info(f"   Classificação: {satisfaction_class}")
            logger.info(f"   Métricas normalizadas: movimento={move_speed_norm:.2f}, cardíaca={heart_rate_norm:.2f}, respiração={breath_rate_norm:.2f}")
            
            return score, satisfaction_class
            
        except Exception as e:
            logger.error(f"❌ Erro ao calcular satisfação: {str(e)}")
            logger.error(traceback.format_exc())
            return 50, 'Neutro'  # Valor padrão em caso de erro

    def calculate_engagement(self, records):
        """
        Calcula engajamento baseado no histórico de registros
        Retorna: 
        - is_engaged: booleano indicando se está engajado
        - duration: duração do engajamento em segundos
        """
        if not records or len(records) < 2:
            logger.info("Não há registros suficientes para calcular engajamento")
            return False, 0

        # Ordenar registros por timestamp
        records = sorted(records, key=lambda x: x['timestamp'])
        
        # Verificar se há movimento contínuo baixo
        engagement_start = None
        last_low_movement = None
        
        for record in records:
            move_speed = float(record['move_speed'])
            current_time = datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S')
            
            if move_speed <= self.MOVEMENT_THRESHOLD:
                if not engagement_start:
                    engagement_start = current_time
                last_low_movement = current_time
            else:
                # Reset se movimento for alto
                engagement_start = None

        # Calcular duração se houver engajamento
        if engagement_start and last_low_movement:
            duration = (last_low_movement - engagement_start).total_seconds()
            is_engaged = duration >= self.ENGAGEMENT_MIN_DURATION
            
            logger.info(f"Engajamento calculado: {is_engaged} (duração: {duration:.1f}s)")
            return is_engaged, int(duration)

        return False, 0

# Instância global do analytics manager
analytics_manager = AnalyticsManager()

class UserSessionManager:
    def __init__(self):
        # Constantes para detecção de entrada/saída
        self.PRESENCE_THRESHOLD = 2.0  # Distância máxima para considerar presença (metros)
        self.MOVEMENT_THRESHOLD = 50.0  # Movimento máximo para considerar "parado" em cm/s
        self.ABSENCE_THRESHOLD = 3.0   # Distância mínima para considerar ausência (metros)
        self.TIME_THRESHOLD = 2        # Tempo mínimo (segundos) para considerar uma nova sessão
        self.DISTANCE_THRESHOLD = 1.0  # Distância mínima entre clientes diferentes (metros)
        
        # Dicionário para armazenar sessões ativas
        self.active_sessions = {}  # {session_id: session_data}
        self.session_positions = {}  # {session_id: (last_x, last_y)}
        
    def find_closest_session(self, x, y, timestamp):
        """
        Encontra a sessão mais próxima das coordenadas fornecidas
        Retorna: (session_id, distance) ou (None, None) se nenhuma sessão próxima for encontrada
        """
        closest_session = None
        min_distance = float('inf')
        
        for session_id, last_pos in self.session_positions.items():
            # Verificar se a sessão não está expirada (mais de 5 segundos sem atualização)
            session_data = self.active_sessions[session_id]
            if (timestamp - session_data['last_update']).total_seconds() > 5:
                continue
                
            # Calcular distância euclidiana
            distance = ((x - last_pos[0]) ** 2 + (y - last_pos[1]) ** 2) ** 0.5
            
            if distance < min_distance:
                min_distance = distance
                closest_session = session_id
        
        return closest_session, min_distance
        
    def detect_session(self, data, timestamp=None):
        """
        Detecta se uma pessoa entrou ou saiu da área da gôndola
        Retorna: (session_id, event_type, session_data)
        event_type pode ser: 'start', 'update', 'end', None
        """
        if timestamp is None:
            try:
                if 'timestamp' in data and data['timestamp']:
                    timestamp = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
                else:
                    timestamp = datetime.now()
            except Exception as e:
                logger.error(f"Erro ao processar timestamp: {str(e)}")
                timestamp = datetime.now()
            
        # Extrair dados relevantes
        x_point = data.get('x_point')
        y_point = data.get('y_point')
        move_speed = data.get('move_speed', 999)
        is_engaged = data.get('is_engaged', 0)
        engagement_duration = data.get('engagement_duration', 0)
        
        # Calcular distância do centro (0,0)
        distance = np.sqrt(x_point**2 + y_point**2) if x_point is not None and y_point is not None else None
        
        # Log para debug
        logger.info(f"Detecção de sessão: x={x_point}, y={y_point}, move_speed={move_speed} cm/s, distância={distance} m")
        
        # Verificar se há dados suficientes
        if distance is None:
            return None, None, None
            
        # Buscar sessão mais próxima
        closest_session_id, session_distance = self.find_closest_session(x_point, y_point, timestamp)
        
        # Se encontrou uma sessão próxima e a distância é menor que o threshold
        if closest_session_id and session_distance <= self.DISTANCE_THRESHOLD:
            session_data = self.active_sessions[closest_session_id]
            session_data['last_update'] = timestamp
            
            # Atualizar posição
            self.session_positions[closest_session_id] = (x_point, y_point)
            
            # Atualizar dados da sessão
            if data.get('heart_rate') is not None:
                session_data['heart_rates'].append(data.get('heart_rate'))
            if data.get('breath_rate') is not None:
                session_data['breath_rates'].append(data.get('breath_rate'))
            if data.get('satisfaction_score') is not None:
                session_data['satisfaction_scores'].append(data.get('satisfaction_score'))
            
            session_data['positions'].append((x_point, y_point))
            session_data['move_speeds'].append(move_speed)
            
            # Verificar engajamento
            if move_speed <= self.MOVEMENT_THRESHOLD:
                if session_data.get('engagement_start_time') is None:
                    session_data['engagement_start_time'] = timestamp
                    session_data['is_engaged'] = 1
                else:
                    eng_duration = (timestamp - session_data['engagement_start_time']).total_seconds()
                    session_data['engagement_duration'] = eng_duration
                    if eng_duration >= 5:
                        session_data['is_engaged'] = 2
            else:
                session_data['engagement_start_time'] = None
            
            # Verificar se a sessão deve ser finalizada
            if distance >= self.ABSENCE_THRESHOLD:
                # Calcular métricas finais
                session_duration = (timestamp - session_data['start_time']).total_seconds()
                if session_duration >= self.TIME_THRESHOLD:
                    session_data['end_time'] = timestamp
                    session_data['duration'] = session_duration
                    session_data['avg_heart_rate'] = np.mean(session_data['heart_rates']) if session_data['heart_rates'] else None
                    session_data['avg_breath_rate'] = np.mean(session_data['breath_rates']) if session_data['breath_rates'] else None
                    session_data['avg_satisfaction'] = np.mean(session_data['satisfaction_scores']) if session_data['satisfaction_scores'] else None
                    
                    # Remover sessão das ativas
                    self.active_sessions.pop(closest_session_id)
                    self.session_positions.pop(closest_session_id)
                    
                    return closest_session_id, 'end', session_data
            
            return closest_session_id, 'update', session_data
            
        # Se não encontrou sessão próxima e está dentro da área de detecção
        elif distance <= self.PRESENCE_THRESHOLD:
            # Criar nova sessão
            new_session_id = str(uuid.uuid4())
            new_session = {
                'session_id': new_session_id,
                'start_time': timestamp,
                'last_update': timestamp,
                'heart_rates': [],
                'breath_rates': [],
                'positions': [(x_point, y_point)],
                'move_speeds': [move_speed],
                'satisfaction_scores': [],
                'is_engaged': 0,
                'engagement_duration': 0,
                'engagement_start_time': None
            }
            
            # Adicionar dados vitais se disponíveis
            if data.get('heart_rate') is not None:
                new_session['heart_rates'].append(data.get('heart_rate'))
            if data.get('breath_rate') is not None:
                new_session['breath_rates'].append(data.get('breath_rate'))
            if data.get('satisfaction_score') is not None:
                new_session['satisfaction_scores'].append(data.get('satisfaction_score'))
            
            # Armazenar nova sessão
            self.active_sessions[new_session_id] = new_session
            self.session_positions[new_session_id] = (x_point, y_point)
            
            logger.info(f"🟢 Nova sessão iniciada: {new_session_id}")
            return new_session_id, 'start', new_session
            
        return None, None, None
        
    def cleanup_expired_sessions(self, current_time):
        """Remove sessões expiradas (sem atualização por mais de 5 segundos)"""
        expired_sessions = []
        
        for session_id, session_data in self.active_sessions.items():
            if (current_time - session_data['last_update']).total_seconds() > 5:
                expired_sessions.append(session_id)
                
        for session_id in expired_sessions:
            session_data = self.active_sessions.pop(session_id)
            self.session_positions.pop(session_id)
            
            # Calcular métricas finais
            session_duration = (current_time - session_data['start_time']).total_seconds()
            if session_duration >= self.TIME_THRESHOLD:
                session_data['end_time'] = current_time
                session_data['duration'] = session_duration
                session_data['avg_heart_rate'] = np.mean(session_data['heart_rates']) if session_data['heart_rates'] else None
                session_data['avg_breath_rate'] = np.mean(session_data['breath_rates']) if session_data['breath_rates'] else None
                session_data['avg_satisfaction'] = np.mean(session_data['satisfaction_scores']) if session_data['satisfaction_scores'] else None
                
                logger.info(f"🔴 Sessão expirada finalizada: {session_id}, duração: {session_duration:.2f}s")
                
                # Salvar sessão no banco de dados
                try:
                    db_manager.save_session_summary(session_data)
                except Exception as e:
                    logger.error(f"Erro ao salvar sessão expirada: {str(e)}")

# Instância global do gerenciador de sessões
user_session_manager = UserSessionManager()

class DataSmoother:
    def __init__(self, window_size=5):
        """
        Inicializa o suavizador de dados
        window_size: tamanho da janela para média móvel
        """
        self.window_size = window_size
        self.heart_rate_history = []
        self.breath_rate_history = []
        
    def smooth_heart_rate(self, heart_rate):
        """Suaviza o valor de heart_rate usando média móvel"""
        if heart_rate is None:
            return None
            
        self.heart_rate_history.append(heart_rate)
        if len(self.heart_rate_history) > self.window_size:
            self.heart_rate_history.pop(0)
            
        if len(self.heart_rate_history) < 2:
            return heart_rate
            
        return sum(self.heart_rate_history) / len(self.heart_rate_history)
        
    def smooth_breath_rate(self, breath_rate):
        """Suaviza o valor de breath_rate usando média móvel"""
        if breath_rate is None:
            return None
            
        self.breath_rate_history.append(breath_rate)
        if len(self.breath_rate_history) > self.window_size:
            self.breath_rate_history.pop(0)
            
        if len(self.breath_rate_history) < 2:
            return breath_rate
            
        return sum(self.breath_rate_history) / len(self.breath_rate_history)
        
    def detect_anomalies(self, heart_rate, breath_rate):
        """
        Detecta anomalias nos dados vitais
        Retorna: (is_heart_anomaly, is_breath_anomaly)
        """
        if not self.heart_rate_history or not self.breath_rate_history:
            return False, False
            
        # Calcular médias e desvios padrão
        heart_mean = sum(self.heart_rate_history) / len(self.heart_rate_history)
        breath_mean = sum(self.breath_rate_history) / len(self.breath_rate_history)
        
        heart_std = (sum((x - heart_mean) ** 2 for x in self.heart_rate_history) / len(self.heart_rate_history)) ** 0.5
        breath_std = (sum((x - breath_mean) ** 2 for x in self.breath_rate_history) / len(self.breath_rate_history)) ** 0.5
        
        # Definir limites para detecção de anomalias (2 desvios padrão)
        heart_threshold = 2 * heart_std
        breath_threshold = 2 * breath_std
        
        # Verificar se os valores atuais são anomalias
        is_heart_anomaly = heart_rate is not None and abs(heart_rate - heart_mean) > heart_threshold
        is_breath_anomaly = breath_rate is not None and abs(breath_rate - breath_mean) > breath_threshold
        
        return is_heart_anomaly, is_breath_anomaly

# Instância global do suavizador de dados
data_smoother = DataSmoother()

class AdaptiveSampler:
    def __init__(self):
        # Configurações de amostragem
        self.HIGH_ACTIVITY_THRESHOLD = 30.0  # cm/s - acima disso é considerado movimento significativo
        self.LOW_ACTIVITY_THRESHOLD = 10.0   # cm/s - abaixo disso é considerado movimento mínimo
        
        # Intervalos de amostragem em milissegundos
        self.HIGH_ACTIVITY_INTERVAL = 200    # 5 amostras por segundo para atividade alta
        self.MEDIUM_ACTIVITY_INTERVAL = 500  # 2 amostras por segundo para atividade média
        self.LOW_ACTIVITY_INTERVAL = 1000    # 1 amostra por segundo para atividade baixa
        self.IDLE_INTERVAL = 2000            # 1 amostra a cada 2 segundos para inatividade
        
        # Estado atual
        self.current_sampling_interval = self.MEDIUM_ACTIVITY_INTERVAL
        self.last_sample_time = None
        self.last_movement_speed = 0
        self.consecutive_idle_count = 0
        self.max_idle_count = 5  # Número máximo de amostras consecutivas em estado de inatividade
        
    def should_sample(self, current_time, movement_speed):
        """
        Determina se devemos coletar uma amostra com base na atividade atual
        Retorna: (bool, int) - se deve amostrar e o próximo intervalo recomendado
        """
        # Na primeira chamada, sempre amostrar
        if self.last_sample_time is None:
            self.last_sample_time = current_time
            return True, self.MEDIUM_ACTIVITY_INTERVAL
        
        # Calcular tempo decorrido desde a última amostra
        elapsed_time = (current_time - self.last_sample_time).total_seconds() * 1000  # em ms
        
        # Determinar o intervalo de amostragem com base na velocidade de movimento
        if movement_speed > self.HIGH_ACTIVITY_THRESHOLD:
            # Atividade alta - amostragem frequente
            self.current_sampling_interval = self.HIGH_ACTIVITY_INTERVAL
            self.consecutive_idle_count = 0
            logger.info(f"Atividade alta detectada: {movement_speed:.1f} cm/s - amostragem a cada {self.current_sampling_interval} ms")
        elif movement_speed > self.LOW_ACTIVITY_THRESHOLD:
            # Atividade média - amostragem normal
            self.current_sampling_interval = self.MEDIUM_ACTIVITY_INTERVAL
            self.consecutive_idle_count = 0
            logger.info(f"Atividade média detectada: {movement_speed:.1f} cm/s - amostragem a cada {self.current_sampling_interval} ms")
        else:
            # Atividade baixa ou inatividade
            if movement_speed <= self.LOW_ACTIVITY_THRESHOLD / 2:
                # Incrementar contador de inatividade
                self.consecutive_idle_count += 1
                
                # Após várias amostras consecutivas de inatividade, reduzir ainda mais a frequência
                if self.consecutive_idle_count >= self.max_idle_count:
                    self.current_sampling_interval = self.IDLE_INTERVAL
                    logger.info(f"Inatividade prolongada: {movement_speed:.1f} cm/s - amostragem a cada {self.current_sampling_interval} ms")
                else:
                    self.current_sampling_interval = self.LOW_ACTIVITY_INTERVAL
                    logger.info(f"Atividade baixa detectada: {movement_speed:.1f} cm/s - amostragem a cada {self.current_sampling_interval} ms")
            else:
                self.current_sampling_interval = self.LOW_ACTIVITY_INTERVAL
                logger.info(f"Atividade baixa detectada: {movement_speed:.1f} cm/s - amostragem a cada {self.current_sampling_interval} ms")
        
        # Verificar se o tempo decorrido é maior que o intervalo atual
        should_sample = elapsed_time >= self.current_sampling_interval
        
        # Mudanças abruptas na velocidade sempre devem ser amostradas
        if abs(movement_speed - self.last_movement_speed) > self.HIGH_ACTIVITY_THRESHOLD:
            logger.info(f"Mudança abrupta na velocidade detectada: {self.last_movement_speed:.1f} -> {movement_speed:.1f} cm/s")
            should_sample = True
        
        # Atualizar estado se for amostrar
        if should_sample:
            self.last_sample_time = current_time
            self.last_movement_speed = movement_speed
        
        return should_sample, self.current_sampling_interval
        
    def reset(self):
        """Reinicia o estado do amostrador"""
        self.last_sample_time = None
        self.last_movement_speed = 0
        self.consecutive_idle_count = 0
        self.current_sampling_interval = self.MEDIUM_ACTIVITY_INTERVAL

# Instância global do amostrador adaptativo
adaptive_sampler = AdaptiveSampler()

class ZoneManager:
    def __init__(self):
        # Constantes para análise comportamental
        self.HESITATION_SPEED_THRESHOLD = 5.0  # cm/s
        self.HESITATION_TIME_THRESHOLD = 3.0   # segundos
        self.INTERACTION_TIME_THRESHOLD = 5.0   # segundos
        self.CONSIDERATION_TIME_THRESHOLD = 10.0 # segundos
        
    def get_zone_at_position(self, x, y):
        """Identifica a zona baseado nas coordenadas (x, y)"""
        try:
            # Buscar áreas ativas que contenham o ponto (x, y)
            query = """
                SELECT * FROM areas
                WHERE is_active = TRUE
                AND x_start <= %s AND x_end >= %s
                AND y_start <= %s AND y_end >= %s
                ORDER BY ABS(x_start - %s) + ABS(y_start - %s)
                LIMIT 1
            """
            
            params = (x, x, y, y, x, y)
            
            db_manager.cursor.execute(query, params)
            area = db_manager.cursor.fetchone()
            
            if area:
                # Calcular distância do ponto ao centro da área
                center_x = (area['x_start'] + area['x_end']) / 2
                center_y = (area['y_start'] + area['y_end']) / 2
                distance = ((x - center_x) ** 2 + (y - center_y) ** 2) ** 0.5
                
                return {
                    'area_id': area['id'],
                    'area_name': area['area_name'],
                    'description': area['description'],
                    'distance': distance
                }
            
            return {
                'area_name': 'FORA_ALCANCE',
                'description': 'Área fora do alcance de monitoramento',
                'distance': abs(y)
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar área: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'area_name': 'ERRO',
                'description': 'Erro ao identificar área',
                'distance': 0
            }
        
    def analyze_behavior(self, x, y, move_speed, timestamp, area_id):
        """Analisa o comportamento do cliente na área"""
        try:
            # Determinar profundidade de interação baseado na velocidade
            if move_speed <= self.HESITATION_SPEED_THRESHOLD:
                interaction_depth = 'INTERACAO'
            elif move_speed <= self.HESITATION_SPEED_THRESHOLD * 2:
                interaction_depth = 'CONSIDERACAO'
            elif move_speed <= self.HESITATION_SPEED_THRESHOLD * 3:
                interaction_depth = 'ATENCAO'
            else:
                interaction_depth = 'PASSAGEM'
            
            # Determinar padrão de comportamento baseado na velocidade
            if move_speed <= self.HESITATION_SPEED_THRESHOLD:
                behavior_pattern = 'INTERESSE_ALTO'
            elif move_speed <= self.HESITATION_SPEED_THRESHOLD * 2:
                behavior_pattern = 'INTERESSE_MEDIO'
            elif move_speed <= self.HESITATION_SPEED_THRESHOLD * 3:
                behavior_pattern = 'INTERESSE_BAIXO'
            else:
                behavior_pattern = 'PASSAGEM_RAPIDA'
            
            return {
                'area_id': area_id,
                'interaction_depth': interaction_depth,
                'behavior_pattern': behavior_pattern,
                'move_speed': move_speed,
                'timestamp': timestamp
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao analisar comportamento: {str(e)}")
            logger.error(traceback.format_exc())
            return None

class AreaManager:
    """Gerenciador de áreas de engajamento do cliente"""
    
    def __init__(self):
        self.areas = {
            'PASSAGEM': {
                'y_min': 0.5,  # Mais afastado
                'y_max': float('inf'),
                'speed_threshold': 0.5,  # Velocidade mais alta
                'description': 'Cliente apenas passando'
            },
            'ATENCAO': {
                'y_min': 0.3,
                'y_max': 0.5,
                'speed_threshold': 0.3,  # Velocidade moderada
                'description': 'Cliente olhando de longe'
            },
            'CONSIDERACAO': {
                'y_min': 0.15,
                'y_max': 0.3,
                'speed_threshold': 0.2,  # Velocidade mais baixa
                'description': 'Cliente analisando produtos'
            },
            'INTERACAO': {
                'y_min': 0.0,
                'y_max': 0.15,  # Mais próximo
                'speed_threshold': 0.1,  # Velocidade muito baixa
                'description': 'Cliente próximo, possivelmente pegando produto'
            }
        }
        
    def get_area_at_position(self, x: float, y: float, speed: float) -> dict:
        """Identifica a área baseada na posição Y e velocidade"""
        for area_name, area in self.areas.items():
            if area['y_min'] <= y < area['y_max'] and speed >= area['speed_threshold']:
                return {
                    'area_name': area_name,
                    'description': area['description'],
                    'y_distance': y,
                    'speed': speed
                }
        return None
        
    def analyze_behavior(self, x: float, y: float, speed: float, timestamp: datetime, current_area: str) -> dict:
        """Analisa o comportamento do cliente na área atual"""
        if not current_area:
            return None
            
        # Identificar padrão de comportamento
        if speed < 0.1:
            behavior_pattern = 'PARADO'
        elif speed < 0.3:
            behavior_pattern = 'ANALISANDO'
        else:
            behavior_pattern = 'PASSANDO'
            
        # Determinar profundidade de interação
        if y <= 0.15:
            interaction_depth = 'INTERACAO'
        elif y <= 0.3:
            interaction_depth = 'CONSIDERACAO'
        elif y <= 0.5:
            interaction_depth = 'ATENCAO'
        else:
            interaction_depth = 'PASSAGEM'
            
        return {
            'area_name': current_area,
            'behavior_pattern': behavior_pattern,
            'interaction_depth': interaction_depth,
            'y_distance': y,
            'speed': speed,
            'timestamp': timestamp
        }

# Instância global do gerenciador de áreas
area_manager = AreaManager()

@app.route('/radar/data', methods=['POST'])
def receive_radar_data():
    """Endpoint para receber dados do radar"""
    try:
        # Obter dados do request
        data = request.get_json()
        current_time = datetime.now()
        
        logger.info("==================================================")
        logger.info("📡 Requisição POST recebida em /radar/data")
        logger.info(f"Headers: {request.headers}")
        logger.info(f"Dados recebidos: {data}")
        
        # Converter dados
        converted_data = convert_radar_data(data)
        if not converted_data:
            return jsonify({
                "status": "error",
                "message": "Dados inválidos"
            }), 400

        # Verificar política de amostragem
        should_sample, next_interval = adaptive_sampler.should_sample(
            current_time, 
            converted_data['move_speed']
        )

        if not should_sample:
            logger.info(f"Amostra ignorada pela política de amostragem. Próximo intervalo: {next_interval}ms")
            return jsonify({
                "status": "success",
                "message": "Amostra ignorada pela política de amostragem",
                "next_sample_interval_ms": next_interval
            })
            
        # Adicionar timestamp
        converted_data['timestamp'] = current_time.strftime('%Y-%m-%d %H:%M:%S')

        # Identificar área atual
        area = area_manager.get_area_at_position(
            converted_data['x_point'],
            converted_data['y_point'],
            converted_data['move_speed']
        )
        logger.info(f"🎯 Área atual: {area['area_name']} (distância: {area['distance']:.2f}m)")
        
        # Identificar seção baseado na posição
        section = shelf_manager.get_section_at_position(
            converted_data['x_point'],
            converted_data['y_point']
        )
        
        if section:
            converted_data['section_id'] = section['section_id']
            converted_data['product_id'] = section['product_id']
            logger.info(f"📍 Seção detectada: {section['name']} (Produto: {section['product_id']})")
        else:
            converted_data['section_id'] = None
            converted_data['product_id'] = None
            
        # Adicionar informação da área
        converted_data['area'] = area['area_name']
        
        # Obter últimos registros para calcular engajamento
        last_records = db_manager.get_last_records(10)
        
        # Calcular engajamento
        is_engaged, engagement_duration = analytics_manager.calculate_engagement(last_records)
        converted_data['is_engaged'] = is_engaged
        converted_data['engagement_duration'] = engagement_duration
        
        # Calcular satisfação
        satisfaction_data = analytics_manager.calculate_satisfaction_score(
            converted_data.get('move_speed'),
            converted_data.get('heart_rate'),
            converted_data.get('breath_rate')
        )
        
        converted_data['satisfaction_score'] = satisfaction_data[0]
        converted_data['satisfaction_class'] = satisfaction_data[1]
        
        # Log dos dados calculados
        logger.info(f"Dados de engajamento: engajado={is_engaged}, duração={engagement_duration}s")
        logger.info(f"Dados de satisfação: score={satisfaction_data[0]}, class={satisfaction_data[1]}")
        
        # Inserir dados no banco
        success = db_manager.insert_radar_data(converted_data)
        
        if not success:
            logger.error("❌ Falha ao inserir dados no banco")
            return jsonify({
                "status": "error",
                "message": "Falha ao inserir dados no banco"
            }), 500
        
        return jsonify({
            "status": "success",
            "message": "Dados processados com sucesso",
            "data": converted_data,
            "next_sample_interval_ms": next_interval
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar dados: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro ao processar dados: {str(e)}"
        }), 500

@app.route('/radar/status', methods=['GET'])
def get_status():
    """Endpoint para verificar status"""
    try:
        status = {
            "server": "online",
            "database": "offline",
            "last_records": None,
            "connection_info": {}
        }

        try:
            # Verificar conexão
            if db_manager and db_manager.conn:
                is_connected = db_manager.conn.is_connected()
                status["connection_info"]["is_connected"] = is_connected
                
                if is_connected:
                    status["database"] = "online"
                    status["last_records"] = db_manager.get_last_records(5)
                    
                    # Obter informações do servidor
                    try:
                        cursor = db_manager.conn.cursor(dictionary=True)
                        cursor.execute("SELECT VERSION() as version")
                        version = cursor.fetchone()
                        cursor.close()
                        
                        if version:
                            status["connection_info"]["version"] = version["version"]
                    except Exception as e:
                        status["connection_info"]["version_error"] = str(e)
                else:
                    status["connection_info"]["connection_error"] = "Connection object exists but is not connected"
            else:
                status["connection_info"]["error"] = "Database manager or connection object is None"
        except Exception as e:
            status["connection_info"]["exception"] = str(e)

        return jsonify(status)
    except Exception as e:
        logger.error(f"Erro ao verificar status: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
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

@app.route('/shelf/sections', methods=['GET'])
def get_sections():
    """Endpoint para listar todas as seções"""
    try:
        if not db_manager:
            return jsonify({
                "status": "error",
                "message": "Banco de dados não disponível"
            }), 500
            
        sections = shelf_manager.get_all_sections(db_manager)
        
        return jsonify({
            "status": "success",
            "count": len(sections),
            "sections": sections
        })
    except Exception as e:
        logger.error(f"Erro ao listar seções: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno: {str(e)}"
        }), 500

@app.route('/shelf/sections', methods=['POST'])
def add_section():
    """Endpoint para adicionar uma nova seção"""
    try:
        if not request.is_json:
            return jsonify({
                "status": "error",
                "message": "Content-Type deve ser application/json"
            }), 400
            
        section_data = request.get_json()
        
        # Validar dados obrigatórios
        required_fields = ['section_name', 'x_start', 'y_start', 'x_end', 'y_end', 'product_id', 'product_name']
        for field in required_fields:
            if field not in section_data:
                return jsonify({
                    "status": "error",
                    "message": f"Campo obrigatório não fornecido: {field}"
                }), 400
        
        if not db_manager:
            return jsonify({
                "status": "error",
                "message": "Banco de dados não disponível"
            }), 500
            
        success = shelf_manager.add_section(section_data, db_manager)
        
        if success:
            return jsonify({
                "status": "success",
                "message": "Seção adicionada com sucesso"
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Erro ao adicionar seção"
            }), 500
            
    except Exception as e:
        logger.error(f"Erro ao adicionar seção: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno: {str(e)}"
        }), 500

@app.route('/shelf/sections/<int:section_id>', methods=['PUT'])
def update_section(section_id):
    """Endpoint para atualizar uma seção existente"""
    try:
        if not request.is_json:
            return jsonify({
                "status": "error",
                "message": "Content-Type deve ser application/json"
            }), 400
            
        section_data = request.get_json()
        
        if not db_manager:
            return jsonify({
                "status": "error",
                "message": "Banco de dados não disponível"
            }), 500
            
        success = shelf_manager.update_section(section_id, section_data, db_manager)
        
        if success:
            return jsonify({
                "status": "success",
                "message": "Seção atualizada com sucesso"
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Erro ao atualizar seção"
            }), 500
            
    except Exception as e:
        logger.error(f"Erro ao atualizar seção: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno: {str(e)}"
        }), 500

@app.route('/radar/sampling/config', methods=['GET'])
def get_sampling_config():
    """Retorna a configuração atual do amostrador adaptativo"""
    try:
        config = {
            "high_activity_threshold": adaptive_sampler.HIGH_ACTIVITY_THRESHOLD,
            "low_activity_threshold": adaptive_sampler.LOW_ACTIVITY_THRESHOLD,
            "high_activity_interval_ms": adaptive_sampler.HIGH_ACTIVITY_INTERVAL,
            "medium_activity_interval_ms": adaptive_sampler.MEDIUM_ACTIVITY_INTERVAL,
            "low_activity_interval_ms": adaptive_sampler.LOW_ACTIVITY_INTERVAL,
            "idle_interval_ms": adaptive_sampler.IDLE_INTERVAL,
            "max_idle_count": adaptive_sampler.max_idle_count,
            "current_sampling_interval_ms": adaptive_sampler.current_sampling_interval,
            "consecutive_idle_count": adaptive_sampler.consecutive_idle_count
        }
        
        return jsonify({
            "status": "success",
            "config": config
        })
    except Exception as e:
        logger.error(f"Erro ao obter configuração de amostragem: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno: {str(e)}"
        }), 500

@app.route('/radar/sampling/config', methods=['POST'])
def update_sampling_config():
    """Atualiza a configuração do amostrador adaptativo"""
    try:
        if not request.is_json:
            return jsonify({
                "status": "error",
                "message": "Content-Type deve ser application/json"
            }), 400
            
        config = request.get_json()
        
        # Atualizar parâmetros
        if 'high_activity_threshold' in config:
            adaptive_sampler.HIGH_ACTIVITY_THRESHOLD = float(config['high_activity_threshold'])
            
        if 'low_activity_threshold' in config:
            adaptive_sampler.LOW_ACTIVITY_THRESHOLD = float(config['low_activity_threshold'])
            
        if 'high_activity_interval_ms' in config:
            adaptive_sampler.HIGH_ACTIVITY_INTERVAL = int(config['high_activity_interval_ms'])
            
        if 'medium_activity_interval_ms' in config:
            adaptive_sampler.MEDIUM_ACTIVITY_INTERVAL = int(config['medium_activity_interval_ms'])
            
        if 'low_activity_interval_ms' in config:
            adaptive_sampler.LOW_ACTIVITY_INTERVAL = int(config['low_activity_interval_ms'])
            
        if 'idle_interval_ms' in config:
            adaptive_sampler.IDLE_INTERVAL = int(config['idle_interval_ms'])
            
        if 'max_idle_count' in config:
            adaptive_sampler.max_idle_count = int(config['max_idle_count'])
        
        # Resetar o estado do amostrador ao aplicar novas configurações
        adaptive_sampler.reset()
        
        logger.info(f"Configuração de amostragem atualizada: {config}")
        
        return jsonify({
            "status": "success",
            "message": "Configuração atualizada com sucesso",
            "config": {
                "high_activity_threshold": adaptive_sampler.HIGH_ACTIVITY_THRESHOLD,
                "low_activity_threshold": adaptive_sampler.LOW_ACTIVITY_THRESHOLD,
                "high_activity_interval_ms": adaptive_sampler.HIGH_ACTIVITY_INTERVAL,
                "medium_activity_interval_ms": adaptive_sampler.MEDIUM_ACTIVITY_INTERVAL,
                "low_activity_interval_ms": adaptive_sampler.LOW_ACTIVITY_INTERVAL,
                "idle_interval_ms": adaptive_sampler.IDLE_INTERVAL,
                "max_idle_count": adaptive_sampler.max_idle_count,
                "current_sampling_interval_ms": adaptive_sampler.current_sampling_interval
            }
        })
    except Exception as e:
        logger.error(f"Erro ao atualizar configuração de amostragem: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno: {str(e)}"
        }), 500

# Instância global do gerenciador de zonas
zone_manager = ZoneManager()

if __name__ == "__main__":
    port = 3000  # Porta fixa em 3000
    host = "0.0.0.0"
    
    print("\n" + "="*50)
    print("🚀 Servidor Radar iniciando...")
    print(f"📡 Endpoint dados: http://{host}:{port}/radar/data")
    print(f"ℹ️  Endpoint status: http://{host}:{port}/radar/status")
    print(f"👥 Endpoint sessões: http://{host}:{port}/radar/sessions")
    print(f"👤 Endpoint sessão específica: http://{host}:{port}/radar/sessions/<session_id>")
    print(f"⚙️  Endpoint configuração amostragem: http://{host}:{port}/radar/sampling/config")
    print(f"🛒 Endpoint seções da gôndola: http://{host}:{port}/shelf/sections")
    print(f"📍 Endpoint zonas: http://{host}:{port}/zones")
    print(f"📍 Endpoint áreas: http://{host}:{port}/areas")
    print("⚡ Use Ctrl+C para encerrar")
    print("="*50 + "\n")
    
    # Informações sobre a amostragem adaptativa
    print("📊 Sistema de Amostragem Adaptativa Ativado")
    print(f"   - Atividade alta (> {adaptive_sampler.HIGH_ACTIVITY_THRESHOLD} cm/s): {adaptive_sampler.HIGH_ACTIVITY_INTERVAL} ms")
    print(f"   - Atividade média: {adaptive_sampler.MEDIUM_ACTIVITY_INTERVAL} ms")
    print(f"   - Atividade baixa (< {adaptive_sampler.LOW_ACTIVITY_THRESHOLD} cm/s): {adaptive_sampler.LOW_ACTIVITY_INTERVAL} ms")
    print(f"   - Inatividade prolongada: {adaptive_sampler.IDLE_INTERVAL} ms")
    print("="*50 + "\n")
    
    app.run(host=host, port=port, debug=True) 
