import mysql.connector
from datetime import datetime
import logging
import json
from typing import Dict
import os
from dotenv import load_dotenv
from flask import Flask, request, jsonify
import threading

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='radar_metrics.log'
)

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do MySQL
db_config = {
    "host": os.getenv("DB_HOST", "168.75.89.11"),
    "user": os.getenv("DB_USER", "belugaDB"),
    "password": os.getenv("DB_PASSWORD", "Rpcr@300476"),
    "database": os.getenv("DB_NAME", "Beluga_Analytics"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "ssl_disabled": True  # Desativa SSL
}

app = Flask(__name__)

class RadarDataHandler:
    def __init__(self):
        self.mysql_manager = MySQLManager()
        self.current_sequence_id = 0
        self.last_move_speed = None

    def process_radar_data(self, data: Dict) -> Dict:
        """Processa dados recebidos do radar ESP"""
        try:
            logging.info(f"Iniciando processamento dos dados: {json.dumps(data)}")
            
            # Processa a velocidade para determinar sequência de engajamento
            if data["move_speed"] == 0:
                if self.last_move_speed is None or self.last_move_speed > 0:
                    self.current_sequence_id += 1
                data["sequencia_engajamento"] = self.current_sequence_id
            else:
                data["sequencia_engajamento"] = None

            self.last_move_speed = data["move_speed"]

            # Adiciona timestamp
            data["timestamp"] = datetime.now().isoformat()
            
            logging.info(f"Dados processados, enviando para o banco: {json.dumps(data)}")

            # Salva no banco de dados
            self.mysql_manager.insert_interacao(data)

            return {"status": "success", "message": "Dados processados com sucesso"}

        except Exception as e:
            logging.error(f"Erro ao processar dados do radar: {e}")
            return {"status": "error", "message": str(e)}

class MySQLManager:
    def __init__(self):
        self.conn = mysql.connector.connect(**db_config)
        self.cursor = self.conn.cursor()
        self._create_tables()

        # Inicia thread de reconexão
        self.keep_alive_thread = threading.Thread(target=self._keep_alive, daemon=True)
        self.keep_alive_thread.start()

    def _keep_alive(self):
        """Mantém conexão com MySQL ativa"""
        while True:
            try:
                if not self.conn.is_connected():
                    self.conn.ping(reconnect=True)
                threading.Event().wait(60)  # Verifica a cada minuto
            except Exception as e:
                logging.error(f"Erro na conexão MySQL: {e}")
                try:
                    self.conn = mysql.connector.connect(**db_config)
                    self.cursor = self.conn.cursor()
                except Exception as e:
                    logging.error(f"Falha na reconexão: {e}")

    def _create_tables(self):
        """Cria as tabelas necessárias"""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS radar_interacoes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                x_point FLOAT,
                y_point FLOAT,
                move_speed FLOAT,
                heart_rate FLOAT NULL,
                breath_rate FLOAT NULL,
                timestamp DATETIME,
                sequencia_engajamento INT NULL
            )
        """)
        self.conn.commit()

    def insert_interacao(self, data: Dict):
        """Insere uma nova interação"""
        try:
            if not self.conn.is_connected():
                logging.warning("Conexão perdida com o MySQL. Tentando reconectar...")
                self.conn.ping(reconnect=True)
                
            sql = """
                INSERT INTO radar_interacoes
                (x_point, y_point, move_speed, heart_rate, breath_rate, timestamp, sequencia_engajamento)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                data['x_point'],
                data['y_point'],
                data['move_speed'],
                data.get('heart_rate'),
                data.get('breath_rate'),
                data['timestamp'],
                data.get('sequencia_engajamento')
            )
            
            logging.info(f"Executando SQL: {sql}")
            logging.info(f"Valores: {values}")
            
            self.cursor.execute(sql, values)
            self.conn.commit()
            
            logging.info("Dados inseridos com sucesso no MySQL")
            
        except mysql.connector.Error as err:
            logging.error(f"Erro MySQL ao inserir dados: {err}")
            raise
        except Exception as e:
            logging.error(f"Erro geral ao inserir dados: {e}")
            raise

# Instancia o manipulador de dados
radar_handler = RadarDataHandler()

@app.route('/radar/data', methods=['POST'])
def receive_radar_data():
    """Endpoint para receber dados do ESP"""
    try:
        data = request.get_json()
        logging.info(f"Dados recebidos do radar: {json.dumps(data)}")
        result = radar_handler.process_radar_data(data)
        return jsonify(result)
    except Exception as e:
        logging.error(f"Erro ao receber dados do radar: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000)
