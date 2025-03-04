import mysql.connector
from datetime import datetime
import logging
import json
from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
import traceback

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

# Configurações do MySQL sem SSL
db_config = {
    "host": os.getenv("DB_HOST", "168.75.89.11"),
    "user": os.getenv("DB_USER", "belugaDB"),
    "password": os.getenv("DB_PASSWORD", "Rpcr@300476"),
    "database": os.getenv("DB_NAME", "Beluga_Analytics"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "ssl_disabled": True,
    "use_pure": True,
    "auth_plugin": 'mysql_native_password',
    "allow_insecure_ssl": True,
    "ssl": {
        "verify_cert": False,
        "verify_identity": False,
        "ca": None,
        "cert": None,
        "key": None,
    }
}

app = Flask(__name__)

class DatabaseManager:
    def __init__(self):
        try:
            self.connect()
            self.create_tables()
            self.last_sequence = self.get_last_sequence()
            self.last_move_speed = None
        except Exception as e:
            logger.error(f"Erro na inicialização do DatabaseManager: {e}")
            logger.error(traceback.format_exc())
    
    def connect(self):
        """Estabelece conexão com o banco de dados"""
        try:
            logger.info("Tentando conectar ao banco de dados...")
            logger.debug(f"Configurações de conexão: {db_config}")
            self.conn = mysql.connector.connect(**db_config)
            self.cursor = self.conn.cursor(dictionary=True)
            logger.info("✅ Conexão com o banco de dados estabelecida com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao banco de dados: {e}")
            logger.error(traceback.format_exc())
            raise

    def create_tables(self):
        """Cria as tabelas e views necessárias"""
        try:
            logger.info("Verificando/criando tabela radar_interacoes...")
            
            # Criar tabela principal
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
            
            # Criar view de engajamento
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
            logger.info("✅ Tabelas e views criadas/atualizadas com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro ao criar tabelas: {e}")
            logger.error(traceback.format_exc())
            raise

    def get_last_sequence(self):
        """Obtém a última sequência de engajamento"""
        try:
            self.cursor.execute("""
                SELECT MAX(sequencia_engajamento) as last_seq 
                FROM radar_interacoes
            """)
            result = self.cursor.fetchone()
            return result['last_seq'] if result and result['last_seq'] is not None else 0
        except Exception as e:
            logger.error(f"Erro ao obter última sequência: {e}")
            return 0

    def calculate_engagement_sequence(self, move_speed):
        """Calcula a sequência de engajamento"""
        if self.last_move_speed is None:
            self.last_move_speed = move_speed
            return self.last_sequence
        
        # Se mudou de movimento para parado ou vice-versa, incrementa a sequência
        if (self.last_move_speed == 0 and move_speed > 0) or (self.last_move_speed > 0 and move_speed == 0):
            self.last_sequence += 1
        
        self.last_move_speed = move_speed
        return self.last_sequence

    def insert_data(self, data):
        """Insere dados no banco"""
        try:
            if not self.conn.is_connected():
                logger.warning("Reconectando ao banco de dados...")
                self.connect()
            
            # Calcular sequência de engajamento
            move_speed = float(data['move_speed'])
            sequence = self.calculate_engagement_sequence(move_speed)
            
            sql = """
                INSERT INTO radar_interacoes
                (device_id, x_point, y_point, move_speed, heart_rate, breath_rate, sequencia_engajamento)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                str(data.get('device_id', 'UNKNOWN')),
                float(data['x_point']),
                float(data['y_point']),
                move_speed,
                float(data.get('heart_rate', 0)) or None,
                float(data.get('breath_rate', 0)) or None,
                sequence
            )
            
            logger.info("📝 Inserindo dados no banco...")
            logger.debug(f"SQL: {sql}")
            logger.debug(f"Valores: {values}")
            
            self.cursor.execute(sql, values)
            self.conn.commit()
            logger.info("✅ Dados inseridos com sucesso!")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao inserir dados: {e}")
            logger.error(f"Dados recebidos: {data}")
            logger.error(traceback.format_exc())
            self.conn.rollback()
            raise

    def get_last_records(self, limit=5):
        """Retorna os últimos registros inseridos"""
        try:
            if not self.conn.is_connected():
                self.connect()
            
            sql = """
                SELECT 
                    r.*,
                    COALESCE(e.segundos_parado, 0) as segundos_parado
                FROM radar_interacoes r
                LEFT JOIN engajamento e ON r.sequencia_engajamento = e.sequencia_engajamento
                ORDER BY r.timestamp DESC 
                LIMIT %s
            """
            self.cursor.execute(sql, (limit,))
            records = self.cursor.fetchall()
            
            # Converter datetime para string
            for record in records:
                record['timestamp'] = record['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            return records
        except Exception as e:
            logger.error(f"Erro ao buscar registros: {e}")
            logger.error(traceback.format_exc())
            raise

    def get_engagement_stats(self):
        """Retorna estatísticas de engajamento"""
        try:
            sql = """
                SELECT 
                    sequencia_engajamento,
                    segundos_parado,
                    (SELECT COUNT(*) FROM radar_interacoes WHERE sequencia_engajamento = e.sequencia_engajamento) as total_registros
                FROM engajamento e
                ORDER BY sequencia_engajamento DESC
                LIMIT 10
            """
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas de engajamento: {e}")
            logger.error(traceback.format_exc())
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
            data = request.get_json()
            logger.debug(f"Dados recebidos: {data}")
        except Exception as e:
            logger.error(f"❌ Erro ao parsear JSON: {e}")
            return jsonify({
                "status": "error",
                "message": "JSON inválido"
            }), 400

        # Validar dados
        is_valid, error_message = validate_data(data)
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
        db_manager.insert_data(data)
        
        return jsonify({
            "status": "success",
            "message": "Dados processados com sucesso"
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
