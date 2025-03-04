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
    level=logging.DEBUG,  # Mudando para DEBUG para mais detalhes
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('radar.log'),
        logging.StreamHandler()
    ]
)

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do MySQL sem SSL
db_config = {
    "host": os.getenv("DB_HOST", "168.75.89.11"),
    "user": os.getenv("DB_USER", "belugaDB"),
    "password": os.getenv("DB_PASSWORD", "Rpcr@300476"),
    "database": os.getenv("DB_NAME", "Beluga_Analytics"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "use_pure": True,  # Usar driver Python puro
    "ssl_disabled": True  # Desabilitar SSL completamente
}

app = Flask(__name__)

class DatabaseManager:
    def __init__(self):
        try:
            self.connect()
            self.create_table()
        except Exception as e:
            logging.error(f"Erro na inicialização do DatabaseManager: {e}")
            logging.error(traceback.format_exc())
    
    def connect(self):
        """Estabelece conexão com o banco de dados"""
        try:
            self.conn = mysql.connector.connect(**db_config)
            self.cursor = self.conn.cursor()
            logging.info("Conexão com o banco de dados estabelecida")
        except Exception as e:
            logging.error(f"Erro ao conectar ao banco de dados: {e}")
            logging.error(f"Configurações utilizadas: {db_config}")
            logging.error(traceback.format_exc())
            raise

    def create_table(self):
        """Cria a tabela se não existir"""
        try:
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
            logging.info("Tabela radar_dados verificada/criada")
        except Exception as e:
            logging.error(f"Erro ao criar tabela: {e}")
            logging.error(traceback.format_exc())
            raise

    def insert_data(self, data):
        """Insere dados no banco"""
        try:
            if not self.conn.is_connected():
                logging.warning("Reconectando ao banco de dados...")
                self.connect()
            
            sql = """
                INSERT INTO radar_dados
                (x_point, y_point, move_speed, heart_rate, breath_rate, timestamp, device_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                data['x_point'],
                data['y_point'],
                data['move_speed'],
                data.get('heart_rate'),
                data.get('breath_rate'),
                datetime.now(),
                data.get('device_id')
            )
            
            logging.debug(f"Executando SQL: {sql}")
            logging.debug(f"Valores: {values}")
            
            self.cursor.execute(sql, values)
            self.conn.commit()
            logging.info("Dados inseridos com sucesso")
            return True
        except Exception as e:
            logging.error(f"Erro ao inserir dados: {e}")
            logging.error(f"Dados recebidos: {data}")
            logging.error(traceback.format_exc())
            self.conn.rollback()
            raise

# Instância global do gerenciador de banco de dados
try:
    db_manager = DatabaseManager()
except Exception as e:
    logging.error(f"Erro ao criar instância do DatabaseManager: {e}")
    logging.error(traceback.format_exc())
    db_manager = None

@app.route('/radar/data', methods=['POST'])
def receive_radar_data():
    """Endpoint para receber dados do radar"""
    try:
        logging.info("Requisição POST recebida em /radar/data")
        
        # Verificar se os dados são JSON válidos
        if not request.is_json:
            logging.error("Dados recebidos não são JSON válido")
            return jsonify({
                "status": "error",
                "message": "Dados devem ser enviados no formato JSON"
            }), 400
        
        data = request.get_json()
        logging.debug(f"Dados recebidos: {data}")
        
        # Validação básica dos dados
        required_fields = ['x_point', 'y_point', 'move_speed']
        if not all(field in data for field in required_fields):
            missing_fields = [field for field in required_fields if field not in data]
            logging.error(f"Campos obrigatórios faltando: {missing_fields}")
            return jsonify({
                "status": "error",
                "message": f"Campos obrigatórios faltando: {missing_fields}"
            }), 400

        # Verificar se o DatabaseManager foi inicializado corretamente
        if db_manager is None:
            logging.error("DatabaseManager não foi inicializado corretamente")
            return jsonify({
                "status": "error",
                "message": "Erro interno do servidor: Banco de dados não disponível"
            }), 500
        
        # Salva no banco de dados
        db_manager.insert_data(data)
        
        return jsonify({
            "status": "success",
            "message": "Dados processados com sucesso"
        })

    except Exception as e:
        logging.error(f"Erro ao processar dados: {e}")
        logging.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": f"Erro interno do servidor: {str(e)}"
        }), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    print(f"\nServidor iniciando...")
    print(f"Endpoint disponível em: http://{host}:{port}/radar/data")
    print("Use Ctrl+C para encerrar\n")
    
    app.run(host=host, port=port, debug=True)  # Habilitando modo debug 
