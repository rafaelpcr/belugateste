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
    "connect_timeout": 60,
    "use_pure": True
}

app = Flask(__name__)

def convert_radar_data(raw_data):
    """Converte dados do radar para o formato do banco"""
    try:
        logger.debug(f"Convertendo dados brutos: {raw_data}")
        
        # Extrair coordenadas do primeiro alvo
        targets = raw_data.get('targets', [])
        if not targets:
            return None, "Nenhum alvo detectado nos dados"
        
        first_target = targets[0]
        
        # Processar dados
        converted_data = {
            'x_point': float(first_target.get('x', 0)),
            'y_point': float(first_target.get('y', 0)),
            'move_speed': 1 if float(raw_data.get('distance', 0)) > 0 else 0,
            'heart_rate': float(raw_data.get('heart', 0)),
            'breath_rate': float(raw_data.get('breath', 0)),
            'device_id': raw_data.get('device_id', 'UNKNOWN')
        }
        
        logger.debug(f"Dados convertidos: {converted_data}")
        return converted_data, None
    except Exception as e:
        logger.error(f"Erro ao converter dados: {str(e)}")
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

    def insert_data(self, data):
        """Insere dados no banco"""
        try:
            # Calcular sequência
            move_speed = float(data['move_speed'])
            if self.last_move_speed is None:
                self.last_move_speed = move_speed
            elif (self.last_move_speed == 0 and move_speed > 0) or (self.last_move_speed > 0 and move_speed == 0):
                self.last_sequence += 1
            self.last_move_speed = move_speed
            
            # Preparar query
            query = """
                INSERT INTO radar_interacoes
                (device_id, x_point, y_point, move_speed, heart_rate, breath_rate, sequencia_engajamento)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            # Preparar parâmetros
            params = (
                str(data['device_id']),
                float(data['x_point']),
                float(data['y_point']),
                move_speed,
                float(data['heart_rate']) if data['heart_rate'] else None,
                float(data['breath_rate']) if data['breath_rate'] else None,
                self.last_sequence
            )
            
            # Executar inserção
            if not self.conn or not self.conn.is_connected():
                self.connect_with_retry()
            
            logger.debug(f"Executando query: {query}")
            logger.debug(f"Parâmetros: {params}")
            
            self.cursor.execute(query, params)
            self.conn.commit()
            
            logger.info("✅ Dados inseridos com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao inserir dados: {str(e)}")
            logger.error(f"Dados: {data}")
            if self.conn:
                try:
                    self.conn.rollback()
                except:
                    pass
            raise

    def get_last_records(self, limit=5):
        """Obtém últimos registros"""
        try:
            if not self.conn or not self.conn.is_connected():
                self.connect_with_retry()
                
            query = """
                SELECT 
                    r.*,
                    COALESCE(e.segundos_parado, 0) as segundos_parado
                FROM radar_interacoes r
                LEFT JOIN engajamento e ON r.sequencia_engajamento = e.sequencia_engajamento
                ORDER BY r.timestamp DESC 
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

    def get_engagement_stats(self):
        """Obtém estatísticas de engajamento"""
        try:
            if not self.conn or not self.conn.is_connected():
                self.connect_with_retry()
                
            query = """
                SELECT 
                    sequencia_engajamento,
                    segundos_parado,
                    (SELECT COUNT(*) FROM radar_interacoes WHERE sequencia_engajamento = e.sequencia_engajamento) as total_registros
                FROM engajamento e
                ORDER BY sequencia_engajamento DESC
                LIMIT 10
            """
            
            self.cursor.execute(query)
            return self.cursor.fetchall()
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

@app.route('/radar/data', methods=['POST'])
def receive_radar_data():
    """Endpoint para receber dados do radar"""
    try:
        logger.info("📡 Requisição POST recebida em /radar/data")
        
        # Verificar Content-Type
        if not request.is_json:
            return jsonify({
                "status": "error",
                "message": "Content-Type deve ser application/json"
            }), 400

        # Obter e validar dados
        raw_data = request.get_json()
        logger.debug(f"Dados brutos recebidos: {raw_data}")
        
        # Converter dados
        converted_data, error = convert_radar_data(raw_data)
        if error:
            return jsonify({
                "status": "error",
                "message": error
            }), 400
        
        # Verificar DatabaseManager
        if not db_manager:
            return jsonify({
                "status": "error",
                "message": "Banco de dados não disponível"
            }), 500
        
        # Inserir dados
        db_manager.insert_data(converted_data)
        
        return jsonify({
            "status": "success",
            "message": "Dados processados com sucesso",
            "processed_data": converted_data
        })

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
            "last_records": None,
            "engagement_stats": None
        }

        if db_manager and db_manager.conn and db_manager.conn.is_connected():
            status["database"] = "online"
            status["last_records"] = db_manager.get_last_records(5)
            status["engagement_stats"] = db_manager.get_engagement_stats()

        return jsonify(status)
    except Exception as e:
        logger.error(f"Erro ao verificar status: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/radar/engagement', methods=['GET'])
def get_engagement():
    """Endpoint para estatísticas de engajamento"""
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
        logger.error(f"Erro ao buscar engajamento: {str(e)}")
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
