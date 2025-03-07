# -*- coding: utf-8 -*-
"""MVP-Beluga- Eng + Csat

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1MLiMDJDdQgcKLRrPgbaG3KAnXUr8efRg
"""

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
            'heart_rate': float(raw_data.get('heart_rate', 0)),
            'breath_rate': float(raw_data.get('breath_rate', 0))
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
            # Criar tabela com campos adicionais
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
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            self.conn.commit()
            logger.info("✅ Banco de dados inicializado com sucesso!")

        except Exception as e:
            logger.error(f"❌ Erro ao inicializar banco: {str(e)}")
            raise

    def insert_data(self, data, analytics_data):
        """Insere dados no banco"""
        try:
            # Preparar query com campos adicionais
            query = """
                INSERT INTO radar_dados
                (x_point, y_point, move_speed, heart_rate, breath_rate,
                 satisfaction_score, satisfaction_class, is_engaged, engagement_duration)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Preparar parâmetros
            params = (
                float(data['x_point']),
                float(data['y_point']),
                float(data['move_speed']),
                float(data['heart_rate']) if data['heart_rate'] else None,
                float(data['breath_rate']) if data['breath_rate'] else None,
                float(analytics_data['satisfaction']['score']),
                analytics_data['satisfaction']['classification'],
                bool(analytics_data['engaged']),
                int(analytics_data.get('engagement_duration', 0))
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
            logger.error(f"Analytics: {analytics_data}")
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
        self.MOVEMENT_THRESHOLD = 0.1  # limite para considerar "parado"

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
        Retorna True se pessoa ficou parada por mais de 5 segundos
        """
        if not records:
            return False

        # Organizar registros por timestamp
        sorted_records = sorted(records, key=lambda x: x['timestamp'])

        # Verificar sequência de registros "parados"
        start_time = None
        for record in sorted_records:
            if float(record['move_speed']) <= self.MOVEMENT_THRESHOLD:
                if start_time is None:
                    start_time = datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S')
                else:
                    current_time = datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S')
                    duration = (current_time - start_time).total_seconds()
                    if duration >= self.ENGAGEMENT_TIME_THRESHOLD:
                        return True
            else:
                start_time = None

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

        # Calcular satisfação
        satisfaction_data = analytics_manager.calculate_satisfaction(
            converted_data.get('heart_rate'),
            converted_data.get('breath_rate')
        )

        # Verificar engajamento
        last_records = db_manager.get_last_records(10)  # Últimos 10 registros para análise
        is_engaged = analytics_manager.calculate_engagement(last_records)

        # Adicionar métricas à resposta
        response_data = {
            "status": "success",
            "message": "Dados processados com sucesso",
            "processed_data": converted_data,
            "analytics": {
                "satisfaction": satisfaction_data,
                "engaged": is_engaged
            }
        }

        # Inserir dados
        db_manager.insert_data(converted_data, response_data['analytics'])

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

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3000))
    host = os.getenv("HOST", "0.0.0.0")

    print("\n" + "="*50)
    print("🚀 Servidor Radar iniciando...")
    print(f"📡 Endpoint dados: http://{host}:{port}/radar/data")
    print(f"ℹ️  Endpoint status: http://{host}:{port}/radar/status")
    print("⚡ Use Ctrl+C para encerrar")
    print("="*50 + "\n")

    app.run(host=host, port=port, debug=True)