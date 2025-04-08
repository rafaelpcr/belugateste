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
        
        # Constantes para validação
        HEART_RATE_MIN = 60
        HEART_RATE_MAX = 100
        BREATH_RATE_MIN = 12
        BREATH_RATE_MAX = 20
        MOVE_SPEED_MIN = 0
        MOVE_SPEED_MAX = 100  # Reduzido para 1 m/s
        COORD_MIN = -2  # -2 metros
        COORD_MAX = 2   # 2 metros
        HEART_BREATH_RATIO_MIN = 3.0
        HEART_BREATH_RATIO_MAX = 5.0
        
        # Processar e validar coordenadas
        try:
            x_point = float(raw_data.get('x_point', raw_data.get('x', 0)))
            y_point = float(raw_data.get('y_point', raw_data.get('y', 0)))
            
            # Validar e normalizar coordenadas
            if abs(x_point) > COORD_MAX or abs(y_point) > COORD_MAX:
                logger.warning(f"Coordenadas fora dos limites: ({x_point}, {y_point})")
                x_point = max(min(x_point, COORD_MAX), COORD_MIN)
                y_point = max(min(y_point, COORD_MAX), COORD_MIN)
                
            # Arredondar para 2 casas decimais
            x_point = round(x_point, 2)
            y_point = round(y_point, 2)
            
        except (ValueError, TypeError) as e:
            logger.error(f"Erro ao converter coordenadas: {str(e)}")
            x_point = 0
            y_point = 0
        
        # Processar e validar move_speed
        try:
            move_speed = float(raw_data.get('move_speed', 0))
            
            # Validar faixa de valores
            if move_speed < MOVE_SPEED_MIN:
                logger.warning(f"Move speed negativo detectado: {move_speed}, ajustando para 0")
                move_speed = 0
            elif move_speed > MOVE_SPEED_MAX:
                logger.warning(f"Move speed muito alto detectado: {move_speed}, ajustando para máximo")
                move_speed = MOVE_SPEED_MAX
            
            # Aplicar suavização para valores muito baixos e arredondar
            if 0 < move_speed < 5:
                move_speed = 5
                logger.info("Move speed muito baixo, ajustando para velocidade mínima")
            
            # Arredondar para número inteiro
            move_speed = round(move_speed)
            
        except (ValueError, TypeError) as e:
            logger.error(f"Erro ao converter move_speed: {str(e)}")
            move_speed = 0
        
        # Processar e validar heart_rate
        try:
            heart_rate = float(raw_data.get('heart_rate', 0))
            
            # Validar valores absurdos
            if heart_rate > 300 or heart_rate < 0:
                logger.warning(f"Heart rate inválido: {heart_rate}, descartando")
                heart_rate = None
            # Validar faixa normal
            elif not (HEART_RATE_MIN <= heart_rate <= HEART_RATE_MAX):
                logger.warning(f"Heart rate fora da faixa aceitável: {heart_rate} bpm")
                if heart_rate < HEART_RATE_MIN:
                    heart_rate = HEART_RATE_MIN
                elif heart_rate > HEART_RATE_MAX:
                    heart_rate = HEART_RATE_MAX
            
            # Arredondar para número inteiro
            if heart_rate is not None:
                heart_rate = round(heart_rate)
                
        except (ValueError, TypeError) as e:
            logger.error(f"Erro ao converter heart_rate: {str(e)}")
            heart_rate = None
        
        # Processar e validar breath_rate
        try:
            breath_rate = float(raw_data.get('breath_rate', 0))
            
            # Validar valores absurdos
            if breath_rate > 60 or breath_rate < 0:
                logger.warning(f"Breath rate inválido: {breath_rate}, descartando")
                breath_rate = None
            # Validar faixa normal
            elif not (BREATH_RATE_MIN <= breath_rate <= BREATH_RATE_MAX):
                logger.warning(f"Breath rate fora da faixa aceitável: {breath_rate} rpm")
                if breath_rate < BREATH_RATE_MIN:
                    breath_rate = BREATH_RATE_MIN
                elif breath_rate > BREATH_RATE_MAX:
                    breath_rate = BREATH_RATE_MAX
            
            # Arredondar para número inteiro
            if breath_rate is not None:
                breath_rate = round(breath_rate)
                
        except (ValueError, TypeError) as e:
            logger.error(f"Erro ao converter breath_rate: {str(e)}")
            breath_rate = None
        
        # Validar consistência entre heart_rate e breath_rate
        if heart_rate is not None and breath_rate is not None and breath_rate > 0:
            ratio = heart_rate / breath_rate
            if not (HEART_BREATH_RATIO_MIN <= ratio <= HEART_BREATH_RATIO_MAX):
                logger.warning(f"Razão heart_rate/breath_rate inconsistente: {ratio:.2f}")
                # Se a razão estiver muito fora, descartar ambos os valores
                heart_rate = None
                breath_rate = None
        
        # Montar dados convertidos
        converted_data = {
            'device_id': 'RADAR_1',
            'x_point': x_point,
            'y_point': y_point,
            'move_speed': move_speed,
            'heart_rate': heart_rate,
            'breath_rate': breath_rate,
            'serial_number': raw_data.get('serial_number', 'RADAR_1')
        }
        
        # Log detalhado dos dados processados
        logger.info(f"✅ Dados convertidos com sucesso:")
        logger.info(f"  - Posição: ({x_point}, {y_point})")
        logger.info(f"  - Velocidade: {move_speed} cm/s")
        logger.info(f"  - Heart Rate: {heart_rate} bpm")
        logger.info(f"  - Breath Rate: {breath_rate} rpm")
        logger.info(f"  - Serial Number: {converted_data['serial_number']}")
        
        return converted_data, None
        
    except Exception as e:
        logger.error(f"❌ Erro ao converter dados: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return None, f"Erro ao converter dados: {str(e)}"

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
            
    def get_section_at_position(self, x, y, db_manager):
        """
        Identifica a seção da gôndola baseado nas coordenadas (x, y)
        Retorna: (section_id, section_name, product_id, product_name) ou None se não encontrar
        """
        try:
            # Buscar seções ativas que contenham o ponto (x, y)
            query = """
                SELECT id, section_name, product_id, product_name
                FROM shelf_sections
                WHERE is_active = TRUE
                AND x_start <= %s AND x_end >= %s
                AND y_start <= %s AND y_end >= %s
                LIMIT 1
            """
            
            db_manager.cursor.execute(query, (x, x, y, y))
            section = db_manager.cursor.fetchone()
            
            if section:
                logger.info(f"Seção encontrada: {section['section_name']} (Produto: {section['product_name']})")
                return section
            else:
                logger.info(f"Nenhuma seção encontrada para as coordenadas (x={x}, y={y})")
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
                db_config = {
                    "host": os.getenv("DB_HOST", "168.75.89.11"),
                    "user": os.getenv("DB_USER", "belugaDB"),
                    "password": os.getenv("DB_PASSWORD", "Rpcr@300476"),
                    "database": os.getenv("DB_NAME", "Beluga_Analytics"),
                    "port": int(os.getenv("DB_PORT", 3306)),
                    "use_pure": True,
                    "ssl_disabled": True,
                    "auth_plugin": "mysql_native_password",
                    "connect_timeout": 120,
                    "pool_size": 3,
                    "charset": "utf8mb4",
                    "collation": "utf8mb4_unicode_ci"
                }
                self.conn = mysql.connector.connect(**db_config)
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
            # Verificar/criar tabela de dispositivos primeiro
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
            
            # Inserir dispositivo padrão se não existir
            logger.info("Verificando dispositivo padrão...")
            self.cursor.execute("""
                INSERT IGNORE INTO Dispositivos (serial_number, nome, tipo)
                VALUES ('RADAR_1', 'Radar Principal', 'RADAR')
            """)
            
            # Inserir dispositivo padrão se não existir
            logger.info("Verificando dispositivo padrão...")
            self.cursor.execute("""
                INSERT IGNORE INTO Dispositivos (serial_number, nome, tipo)
                VALUES ('SERIAL_2', 'Radar Teste 2', 'RADAR')
            """)
            
            # Verificar se a tabela radar_dados existe
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
                        section_id INT,
                        product_id VARCHAR(50),
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        serial_number VARCHAR(50),
                        FOREIGN KEY (serial_number) REFERENCES Dispositivos(serial_number)
                    )
                """)
                logger.info("Tabela radar_dados criada com sucesso!")
            
            # Verificar se a coluna serial_number existe
            self.cursor.execute("SHOW COLUMNS FROM radar_dados LIKE 'serial_number'")
            serial_number_exists = self.cursor.fetchone()
            
            if not serial_number_exists:
                logger.info("Adicionando coluna serial_number...")
                try:
                    # Adicionar coluna com valor padrão
                    self.cursor.execute("""
                        ALTER TABLE radar_dados 
                        ADD COLUMN serial_number VARCHAR(50),
                        ADD FOREIGN KEY (serial_number) REFERENCES Dispositivos(serial_number)
                    """)
                    # Atualizar registros existentes
                    self.cursor.execute("""
                        UPDATE radar_dados 
                        SET serial_number = 'RADAR_1' 
                        WHERE serial_number IS NULL
                    """)
                    logger.info("Coluna serial_number adicionada e atualizada com sucesso!")
                except Exception as e:
                    logger.error(f"Erro ao adicionar coluna serial_number: {str(e)}")
            
            # Verificar e adicionar outras colunas que estão faltando
            logger.info("Verificando outras colunas da tabela radar_dados...")
            
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
                'session_id': 'ADD COLUMN session_id VARCHAR(36)',
                'section_id': 'ADD COLUMN section_id INT',
                'product_id': 'ADD COLUMN product_id VARCHAR(50)'
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
            logger.info("✅ Banco de dados atualizado com sucesso!")
            
            # Adicionar as duas novas seções da gôndola
            logger.info("Adicionando seções da gôndola para o teste 2...")
            self.cursor.execute("""
                INSERT IGNORE INTO shelf_sections 
                (section_name, x_start, y_start, x_end, y_end, product_id, product_name)
                VALUES 
                ('Seção 1 - Teste 2', 0.0, 0.0, 0.5, 0.3, 'PROD001', 'Produto Teste 1'),
                ('Seção 2 - Teste 2', 0.5, 0.0, 1.0, 0.3, 'PROD002', 'Produto Teste 2')
            """)
            
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
            
            # Preparar query com campos adicionais
            query = """
                INSERT INTO radar_dados
                (x_point, y_point, move_speed, heart_rate, breath_rate, 
                 satisfaction_score, satisfaction_class, is_engaged, engagement_duration, 
                 session_id, section_id, product_id, serial_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                data.get('session_id'),
                data.get('section_id'),
                data.get('product_id'),
                serial_number
            )
            
            logger.info(f"Query SQL: {query}")
            logger.info(f"Parâmetros: {params}")
            
            # Executar inserção
            self.cursor.execute(query, params)
            self.conn.commit()
            
            logger.info("✅ Dados inseridos com sucesso!")
            logger.info("="*50)
            return True
            
        except Exception as e:
            logger.error("="*50)
            logger.error(f"❌ Erro ao inserir dados: {str(e)}")
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

    def insert_radar_data(self, data):
        """Insere dados do radar no banco"""
        try:
            logger.info("="*50)
            logger.info("Iniciando inserção de dados no banco...")
            logger.info(f"Dados recebidos: {data}")
            
            # Verificar campos obrigatórios
            required_fields = ['x_point', 'y_point', 'move_speed', 'heart_rate', 'breath_rate']
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
                
            if 'session_id' not in data or data['session_id'] is None:
                data['session_id'] = None
                
            if 'section_id' not in data or data['section_id'] is None:
                data['section_id'] = None
                
            if 'product_id' not in data or data['product_id'] is None:
                data['product_id'] = None
                
            if 'serial_number' not in data or data['serial_number'] is None:
                data['serial_number'] = 'SERIAL_2'
            
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
                data.get('x_point'),
                data.get('y_point'),
                data.get('move_speed'),
                data.get('heart_rate'),
                data.get('breath_rate'),
                data.get('satisfaction_score', 0),
                data.get('satisfaction_class', 'NEUTRA'),
                data.get('is_engaged', False),
                data.get('engagement_duration', 0),
                data.get('session_id'),
                data.get('section_id'),
                data.get('product_id'),
                data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                data.get('serial_number', 'SERIAL_2')
            )
            
            logger.info(f"Query: {query}")
            logger.info(f"Parâmetros: {params}")
            
            # Executar inserção
            self.cursor.execute(query, params)
            self.conn.commit()
            
            logger.info("✅ Dados inseridos com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao inserir dados: {str(e)}")
            logger.error(traceback.format_exc())
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
        self.ENGAGEMENT_TIME_THRESHOLD = 5  # segundos (voltando para 5 segundos)
        self.MOVEMENT_THRESHOLD = 20.0  # limite para considerar "parado" em cm/s
        self.ENGAGEMENT_MIN_DURATION = 5  # duração mínima para considerar engajamento completo (segundos)
        
        # Constantes para satisfação
        self.RESP_RATE_NORMAL_MIN = 6
        self.RESP_RATE_NORMAL_MAX = 12
        self.RESP_RATE_ANXIETY = 20
        
        # Pesos para o cálculo de satisfação
        self.WEIGHT_HEART_RATE = 0.6  # α
        self.WEIGHT_RESP_RATE = 0.4   # β
        
        # Rastreamento de engajamento
        self.engagement_start_time = None
        self.last_movement_time = None
        
    def calculate_engagement(self, records):
        """
        Calcula engajamento baseado no histórico de registros
        Retorna: 
        - 0: Não detectado
        - 1: Engajamento inicial (< 5 segundos)
        - 2: Engajamento completo (>= 5 segundos)
        """
        if not records or len(records) < 2:
            logger.info("Não há registros suficientes para calcular engajamento")
            return 0, 0
            
        # Filtrar registros válidos (com move_speed e timestamp)
        valid_records = []
        current_time = datetime.now()
        
        for record in records:
            if (record.get('move_speed') is not None and 
                record.get('timestamp') is not None):
                try:
                    # Tentar converter o timestamp para datetime
                    record_time = datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S')
                    
                    # Verificar se o registro não é muito antigo (máximo 10 segundos)
                    if (current_time - record_time).total_seconds() <= 10:
                        valid_records.append({
                            'move_speed': float(record['move_speed']),
                            'timestamp': record_time,
                            'x_point': float(record.get('x_point', 0)),
                            'y_point': float(record.get('y_point', 0))
                        })
                except (ValueError, TypeError) as e:
                    logger.error(f"Erro ao processar registro para engajamento: {str(e)}")
                    continue
        
        if len(valid_records) < 2:
            logger.info("Não há registros válidos suficientes para calcular engajamento")
            return 0, 0
        
        # Ordenar registros por timestamp (mais recente primeiro)
        valid_records.sort(key=lambda x: x['timestamp'], reverse=True)
        
        # Verificar engajamento
        engagement_count = 0
        total_movement = 0
        last_position = None
        start_time = None
        
        for record in valid_records:
            current_time = record['timestamp']
            current_position = (record['x_point'], record['y_point'])
            move_speed = record['move_speed']
            
            # Calcular movimento total se houver posição anterior
            if last_position:
                movement = ((current_position[0] - last_position[0])**2 + 
                          (current_position[1] - last_position[1])**2)**0.5
                total_movement += movement
            
            # Verificar critérios de engajamento
            is_engaged = (
                move_speed <= self.MOVEMENT_THRESHOLD and  # Velocidade baixa
                (not last_position or  # Primeiro registro
                 total_movement <= 0.5)  # Movimento total menor que 50cm
            )
            
            if is_engaged:
                engagement_count += 1
                if start_time is None:
                    start_time = current_time
            else:
                # Reset se houver movimento significativo
                engagement_count = 0
                start_time = None
                total_movement = 0
            
            last_position = current_position
            
            # Verificar se temos engajamento suficiente
            if engagement_count >= 2:
                duration = (valid_records[0]['timestamp'] - start_time).total_seconds()
                
                if duration >= self.ENGAGEMENT_MIN_DURATION:
                    logger.info(f"Engajamento COMPLETO detectado! Duração: {duration:.1f}s")
                    return 2, duration
                else:
                    logger.info(f"Engajamento INICIAL detectado! Duração: {duration:.1f}s")
                    return 1, duration
        
        logger.info("Engajamento não detectado")
        return 0, 0

    def calculate_satisfaction(self, heart_rate, breath_rate):
        """
        Calcula nível de satisfação baseado em batimentos cardíacos e respiração
        Retorna: score (0-100) e classificação ('POSITIVA', 'NEUTRA', 'NEGATIVA')
        """
        # Constantes para validação
        HEART_RATE_MIN = 60
        HEART_RATE_MAX = 100
        HEART_RATE_IDEAL_MIN = 65
        HEART_RATE_IDEAL_MAX = 75
        
        BREATH_RATE_MIN = 12
        BREATH_RATE_MAX = 20
        BREATH_RATE_IDEAL_MIN = 12
        BREATH_RATE_IDEAL_MAX = 16
        
        # Inicializar scores
        heart_score = 0
        resp_score = 0
        valid_data = False
        
        # Validar e calcular score de batimentos cardíacos
        if heart_rate is not None and HEART_RATE_MIN <= heart_rate <= HEART_RATE_MAX:
            valid_data = True
            
            # Cálculo mais granular para heart_rate
            if HEART_RATE_IDEAL_MIN <= heart_rate <= HEART_RATE_IDEAL_MAX:
                # Score máximo para faixa ideal
                heart_score = 100
            elif heart_rate < HEART_RATE_IDEAL_MIN:
                # Penalidade progressiva abaixo do ideal
                heart_score = 100 - ((HEART_RATE_IDEAL_MIN - heart_rate) / 5) * 20
            else:  # heart_rate > HEART_RATE_IDEAL_MAX
                # Penalidade progressiva acima do ideal
                heart_score = 100 - ((heart_rate - HEART_RATE_IDEAL_MAX) / 5) * 20
            
            # Garantir que o score está entre 0 e 100
            heart_score = max(0, min(100, heart_score))
        
        # Validar e calcular score de respiração
        if breath_rate is not None and BREATH_RATE_MIN <= breath_rate <= BREATH_RATE_MAX:
            valid_data = True
            
            # Cálculo mais granular para breath_rate
            if BREATH_RATE_IDEAL_MIN <= breath_rate <= BREATH_RATE_IDEAL_MAX:
                # Score máximo para faixa ideal
                resp_score = 100
            elif breath_rate > BREATH_RATE_IDEAL_MAX:
                # Penalidade progressiva acima do ideal
                resp_score = 100 - ((breath_rate - BREATH_RATE_IDEAL_MAX) / 2) * 25
            else:  # Não deve acontecer pois BREATH_RATE_IDEAL_MIN = BREATH_RATE_MIN
                resp_score = 100
            
            # Garantir que o score está entre 0 e 100
            resp_score = max(0, min(100, resp_score))
        
        # Se não houver dados válidos, retornar score neutro
        if not valid_data:
            logger.warning("Dados vitais inválidos ou ausentes, retornando score neutro")
            return {
                'score': 50,
                'classification': 'NEUTRA',
                'heart_score': 50,
                'resp_score': 50,
                'is_valid': False
            }
        
        # Calcular score final com pesos
        final_score = (self.WEIGHT_HEART_RATE * heart_score + 
                      self.WEIGHT_RESP_RATE * resp_score)
        
        # Classificar satisfação
        if final_score >= 70:
            classification = 'POSITIVA'
        elif final_score <= 40:
            classification = 'NEGATIVA'
        else:
            classification = 'NEUTRA'
        
        # Log detalhado do cálculo
        logger.info(f"Cálculo de satisfação:")
        logger.info(f"  - Heart Rate: {heart_rate} bpm (score: {heart_score:.2f})")
        logger.info(f"  - Breath Rate: {breath_rate} rpm (score: {resp_score:.2f})")
        logger.info(f"  - Score Final: {final_score:.2f} ({classification})")
            
        return {
            'score': round(final_score, 2),
            'classification': classification,
            'heart_score': round(heart_score, 2),
            'resp_score': round(resp_score, 2),
            'is_valid': True
        }

# Instância global do analytics manager
analytics_manager = AnalyticsManager()

class UserSessionManager:
    def __init__(self):
        # Constantes para detecção de entrada/saída
        self.PRESENCE_THRESHOLD = 2.0  # Distância máxima para considerar presença (metros)
        self.MOVEMENT_THRESHOLD = 50.0  # Movimento máximo para considerar "parado" em cm/s
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
        self.engagement_start_time = None  # Tempo de início do engajamento
        
    def detect_session(self, data, timestamp=None):
        """
        Detecta se uma pessoa entrou ou saiu da área da gôndola
        Retorna: (session_id, event_type, session_data)
        event_type pode ser: 'start', 'update', 'end', None
        """
        if timestamp is None:
            try:
                # Tentar usar o timestamp dos dados
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
        is_engaged = data.get('is_engaged', 0)  # 0=não, 1=inicial, 2=completo
        engagement_duration = data.get('engagement_duration', 0)
        
        # Calcular distância do centro (0,0)
        distance = np.sqrt(x_point**2 + y_point**2) if x_point is not None and y_point is not None else None
        
        # Log para debug
        logger.info(f"Detecção de sessão: x={x_point}, y={y_point}, move_speed={move_speed} cm/s, distância={distance} m, timestamp={timestamp}")
        
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
                        'is_engaged': 0,  # 0=não, 1=inicial, 2=completo
                        'engagement_duration': 0
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
                    
                    # Iniciar rastreamento de engajamento se ainda não iniciado
                    if self.engagement_start_time is None:
                        self.engagement_start_time = timestamp
                        logger.info(f"Início do engajamento registrado: {timestamp}")
                        self.session_data['is_engaged'] = 1  # Engajamento inicial
                    else:
                        # Calcular duração do engajamento
                        engagement_duration = (timestamp - self.engagement_start_time).total_seconds()
                        self.session_data['engagement_duration'] = engagement_duration
                        logger.info(f"Duração do engajamento: {engagement_duration} segundos")
                        
                        # Atualizar status de engajamento baseado na duração
                        if engagement_duration >= 5:  # 5 segundos para engajamento completo
                            self.session_data['is_engaged'] = 2  # Engajamento completo
                            logger.info(f"Engajamento COMPLETO detectado! Duração: {engagement_duration} segundos")
                        else:
                            self.session_data['is_engaged'] = 1  # Engajamento inicial
                            logger.info(f"Engajamento INICIAL mantido. Duração: {engagement_duration} segundos")
                else:
                    # Resetar rastreamento de engajamento se movimento for alto
                    if self.engagement_start_time is not None:
                        logger.info(f"Movimento alto detectado, resetando rastreamento de engajamento")
                        self.engagement_start_time = None
                        
                # Usar o valor de engajamento calculado anteriormente se for maior
                if is_engaged > self.session_data['is_engaged']:
                    self.session_data['is_engaged'] = is_engaged
                    self.session_data['engagement_duration'] = engagement_duration
        else:
            self.consecutive_presence_count = 0
            
            # Resetar rastreamento de engajamento se pessoa sair da área
            if self.engagement_start_time is not None:
                logger.info(f"Pessoa saiu da área, resetando rastreamento de engajamento")
                self.engagement_start_time = None
            
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
                        self.engagement_start_time = None
                        
                        return session_data_copy['session_id'], event_type, session_data_copy
        
        # Atualizar última posição e distância
        self.last_position = (x_point, y_point)
        self.last_distance = distance
        
        return self.current_session_id, event_type, self.session_data

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
            return jsonify({
                "status": "error",
                "message": error
            }), 400
        
        # Timestamp atual
        current_time = datetime.now()
        
        # Verificar se devemos processar esta amostra com base na atividade atual
        move_speed = converted_data.get('move_speed', 0)
        should_sample, next_interval = adaptive_sampler.should_sample(current_time, move_speed)
        
        if not should_sample:
            # Retornar resposta indicando que a amostra foi recebida mas não processada
            return jsonify({
                "status": "success",
                "message": "Amostra ignorada pela política de amostragem adaptativa",
                "next_sample_interval_ms": next_interval
            })
        
        # Registrar que a amostra foi aceita
        logger.info(f"Amostra aceita para processamento. Próximo intervalo: {next_interval} ms")
            
        # Suavizar dados vitais
        heart_rate = data_smoother.smooth_heart_rate(converted_data.get('heart_rate'))
        breath_rate = data_smoother.smooth_breath_rate(converted_data.get('breath_rate'))
        
        # Detectar anomalias
        is_heart_anomaly, is_breath_anomaly = data_smoother.detect_anomalies(heart_rate, breath_rate)
        
        if is_heart_anomaly:
            logger.warning(f"Anomalia detectada em heart_rate: {heart_rate}")
            heart_rate = None
        if is_breath_anomaly:
            logger.warning(f"Anomalia detectada em breath_rate: {breath_rate}")
            breath_rate = None
        
        # Atualizar dados convertidos com valores suavizados
        converted_data['heart_rate'] = heart_rate
        converted_data['breath_rate'] = breath_rate
        
        # Verificar se o banco está disponível
        if not db_manager:
            logger.error("❌ Banco de dados não disponível")
            return jsonify({
                "status": "error",
                "message": "Banco de dados não disponível"
            }), 500
        
        # Adicionar timestamp atual
        converted_data['timestamp'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Identificar seção e produto baseado na posição
        section = shelf_manager.get_section_at_position(
            converted_data['x_point'],
            converted_data['y_point'],
            db_manager
        )
        
        if section:
            converted_data['section_id'] = section['id']
            converted_data['product_id'] = section['product_id']
            logger.info(f"Pessoa detectada na seção: {section['section_name']} (Produto: {section['product_name']})")
        else:
            # Ponto está fora de qualquer seção, vamos ajustar para a seção mais próxima
            logger.info(f"Ponto original ({converted_data['x_point']}, {converted_data['y_point']}) está fora de qualquer seção. Ajustando...")
            
            # Buscar todas as seções
            all_sections = shelf_manager.get_all_sections(db_manager)
            closest_section = None
            min_distance = float('inf')
            
            # Encontrar a seção mais próxima
            for section_item in all_sections:
                # Calcular o centro da seção
                section_center_x = (section_item['x_start'] + section_item['x_end']) / 2
                section_center_y = (section_item['y_start'] + section_item['y_end']) / 2
                
                # Calcular distância do ponto ao centro da seção
                distance = ((converted_data['x_point'] - section_center_x) ** 2 + 
                           (converted_data['y_point'] - section_center_y) ** 2) ** 0.5
                
                # Verificar se é a seção mais próxima
                if distance < min_distance:
                    min_distance = distance
                    closest_section = section_item
            
            if closest_section:
                # Associar à seção mais próxima sem alterar coordenadas
                logger.info(f"Associando à seção mais próxima: {closest_section['section_name']}")
                
                converted_data['section_id'] = closest_section['id']
                converted_data['product_id'] = closest_section['product_id']
                
                logger.info(f"Pessoa associada à seção: {closest_section['section_name']} (Produto: {closest_section['product_name']})")
            else:
                logger.warning("Não foi possível encontrar uma seção próxima")
                converted_data['section_id'] = None
                converted_data['product_id'] = None
        
        # Calcular sessão e engajamento
        session_id, event_type, session_data = user_session_manager.detect_session(converted_data)
        converted_data['session_id'] = session_id
        
        # Salvar resumo da sessão se for final de sessão
        if event_type == 'end' and session_data:
            try:
                db_manager.save_session_summary(session_data)
            except Exception as e:
                logger.error(f"Erro ao salvar resumo da sessão: {str(e)}")
        
        # Obter últimos 5 registros para calcular engajamento
        last_records = db_manager.get_last_records(5)
        
        # Calcular engajamento baseado nos últimos registros
        engagement_level, engagement_duration = analytics_manager.calculate_engagement(last_records)
        converted_data['is_engaged'] = bool(engagement_level)  # 0=não, 1=inicial, 2=completo -> converte para boolean
        converted_data['engagement_duration'] = int(engagement_duration)
        
        # Calcular satisfação
        satisfaction_data = analytics_manager.calculate_satisfaction(
            converted_data.get('heart_rate'), 
            converted_data.get('breath_rate')
        )
        
        converted_data['satisfaction_score'] = satisfaction_data['score']
        converted_data['satisfaction_class'] = satisfaction_data['classification']
        
        # Log dos dados calculados
        logger.info(f"Dados de engajamento: nível={engagement_level}, duração={engagement_duration}s")
        logger.info(f"Dados de satisfação: score={satisfaction_data['score']}, class={satisfaction_data['classification']}")
        
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

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3000))
    host = os.getenv("HOST", "0.0.0.0")
    
    print("\n" + "="*50)
    print("🚀 Servidor Radar iniciando...")
    print(f"📡 Endpoint dados: http://{host}:{port}/radar/data")
    print(f"ℹ️  Endpoint status: http://{host}:{port}/radar/status")
    print(f"👥 Endpoint sessões: http://{host}:{port}/radar/sessions")
    print(f"👤 Endpoint sessão específica: http://{host}:{port}/radar/sessions/<session_id>")
    print(f"⚙️  Endpoint configuração amostragem: http://{host}:{port}/radar/sampling/config")
    print(f"🛒 Endpoint seções da gôndola: http://{host}:{port}/shelf/sections")
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
