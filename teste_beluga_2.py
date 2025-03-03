import mysql.connector
from datetime import datetime
import logging
import json
from typing import Dict
import os
from dotenv import load_dotenv
from flask import Flask, request, jsonify
import threading
import socket
import netifaces
import re

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

class TCPServer:
    def __init__(self, host='0.0.0.0', port=1234):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.radar_handler = RadarDataHandler()
        self.current_data = {}
        
    def start(self):
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            logging.info(f"Servidor TCP iniciado em {self.host}:{self.port}")
            
            # Inicia thread para aceitar conexões
            accept_thread = threading.Thread(target=self._accept_connections)
            accept_thread.daemon = True
            accept_thread.start()
            
        except Exception as e:
            logging.error(f"Erro ao iniciar servidor TCP: {e}")
            raise

    def _accept_connections(self):
        while True:
            try:
                client_socket, address = self.server_socket.accept()
                logging.info(f"Nova conexão de {address}")
                
                # Inicia thread para lidar com o cliente
                client_thread = threading.Thread(target=self._handle_client, args=(client_socket, address))
                client_thread.daemon = True
                client_thread.start()
                
            except Exception as e:
                logging.error(f"Erro ao aceitar conexão: {e}")

    def _handle_client(self, client_socket, address):
        buffer = ""
        device_id = None
        
        try:
            while True:
                data = client_socket.recv(1024).decode('utf-8')
                if not data:
                    break
                
                buffer += data
                
                # Processa o buffer linha por linha
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    self._process_line(line.strip(), device_id)
                    
                    # Extrai device_id se ainda não tiver
                    if not device_id and '[' in line and ']' in line:
                        device_id = re.search(r'\[(.*?)\]', line).group(1)
                        
        except Exception as e:
            logging.error(f"Erro ao processar dados do cliente {address}: {e}")
        finally:
            client_socket.close()
            logging.info(f"Conexão fechada com {address}")

    def _process_line(self, line, device_id):
        try:
            # Extrai valores usando regex
            if 'x_point:' in line:
                self.current_data['x_point'] = float(re.search(r'x_point: ([-\d.]+)', line).group(1))
            elif 'y_point:' in line:
                self.current_data['y_point'] = float(re.search(r'y_point: ([-\d.]+)', line).group(1))
            elif 'move_speed:' in line:
                self.current_data['move_speed'] = float(re.search(r'move_speed: ([-\d.]+)', line).group(1))
            elif 'heart_rate:' in line:
                self.current_data['heart_rate'] = float(re.search(r'heart_rate: ([-\d.]+)', line).group(1))
            elif 'breath_rate:' in line:
                self.current_data['breath_rate'] = float(re.search(r'breath_rate: ([-\d.]+)', line).group(1))
                
                # Se temos todos os dados necessários, processa
                if all(k in self.current_data for k in ['x_point', 'y_point', 'move_speed']):
                    self.current_data['device_id'] = device_id
                    self.radar_handler.process_radar_data(self.current_data.copy())
                    self.current_data.clear()
                    
        except Exception as e:
            logging.error(f"Erro ao processar linha '{line}': {e}")

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
    # Inicia servidor TCP
    tcp_server = TCPServer()
    tcp_server.start()
    
    # Mostra IPs disponíveis
    print("IPs disponíveis no servidor:")
    for interface in netifaces.interfaces():
        try:
            addrs = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addrs:
                for addr in addrs[netifaces.AF_INET]:
                    ip = addr['addr']
                    print(f"Interface {interface}: {ip}")
                    logging.info(f"Interface {interface}: {ip}")
        except Exception as e:
            continue

    print(f"\nServidor TCP rodando na porta 1234")
    print(f"Servidor Flask rodando na porta 8000")
    print("Você pode acessar usando qualquer um dos IPs acima")
    
    # Inicia servidor Flask
    app.run(host='0.0.0.0', port=8000, threaded=True)
