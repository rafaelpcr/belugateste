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
    "use_pure": True,
    "auth_plugin": 'mysql_native_password'
}

app = Flask(__name__)

class DatabaseManager:
    def __init__(self):
        try:
            self.connect()
            self.create_table()
        except Exception as e:
            logger.error(f"Erro na inicialização do DatabaseManager: {e}")
            logger.error(traceback.format_exc())
    
    def connect(self):
        """Estabelece conexão com o banco de dados"""
        try:
            logger.info("Tentando conectar ao banco de dados...")
            logger.debug(f"Configurações de conexão: {db_config}")
            self.conn = mysql.connector.connect(**db_config)
            self.cursor = self.conn.cursor(dictionary=True)  # Retorna resultados como dicionários
            logger.info("✅ Conexão com o banco de dados estabelecida com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao banco de dados: {e}")
            logger.error(traceback.format_exc())
            raise

    def create_table(self):
        """Cria a tabela se não existir"""
        try:
            logger.info("Verificando/criando tabela radar_dados...")
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS radar_dados (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    x_point FLOAT,
                    y_point FLOAT,
                    move_speed FLOAT,
                    heart_rate FLOAT NULL,
                    breath_rate FLOAT NULL,
                    timestamp DATETIME,
                    device_id VARCHAR(50) NULL
                )
            """)
            self.conn.commit()
            logger.info("✅ Tabela radar_dados verificada/criada com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro ao criar tabela: {e}")
            logger.error(traceback.format_exc())
            raise

    def insert_data(self, data):
        """Insere dados no banco"""
        try:
            if not self.conn.is_connected():
                logger.warning("Reconectando ao banco de dados...")
                self.connect()
            
            sql = """
                INSERT INTO radar_dados
                (x_point, y_point, move_speed, heart_rate, breath_rate, timestamp, device_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                float(data['x_point']),  # Convertendo para float
                float(data['y_point']),
                float(data['move_speed']),
                float(data.get('heart_rate', 0)) or None,  # Tratamento para valores nulos
                float(data.get('breath_rate', 0)) or None,
                datetime.now(),
                str(data.get('device_id', 'UNKNOWN'))
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
                SELECT * FROM radar_dados 
                ORDER BY timestamp DESC 
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
            "last_records": None
        }

        if db_manager and db_manager.conn.is_connected():
            status["database"] = "online"
            status["last_records"] = db_manager.get_last_records(5)

        return jsonify(status)
    except Exception as e:
        logger.error(f"Erro ao verificar status: {e}")
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
    print("⚡ Use Ctrl+C para encerrar")
    print("="*50 + "\n")
    
    app.run(host=host, port=port, debug=True) 
