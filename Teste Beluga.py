import mysql.connector
from datetime import datetime
import logging
import json
from typing import Dict, List
import os
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='radar_metrics.log'
)

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do MySQL via variáveis de ambiente
db_config = {
    "host": os.getenv("DB_HOST", "168.75.89.11"),
    "user": os.getenv("DB_USER", "belugaDB"),
    "password": os.getenv("DB_PASSWORD", "Rpcr@300476"),
    "database": os.getenv("DB_NAME", "Beluga_Analytics"),
    "port": os.getenv("DB_PORT", 3306)
}

class RadarDataProcessor:
    def _init_(self, input_file: str):
        self.input_file = input_file
        self.matrix: List[Dict] = []
        self.interactions: List[Dict] = []
        self.current_target: Dict = {}
        self.current_zero_streak = 0
        self.total_engagement_seconds = 0
        
    def read_data(self) -> None:
        """Lê e processa o arquivo de dados do radar"""
        try:
            with open(self.input_file, 'r') as file:
                self.raw_data = file.read()
        except FileNotFoundError:
            logging.error(f"Arquivo {self.input_file} não encontrado")
            raise
            
    def process_line(self, line: str) -> None:
        """Processa uma linha individual dos dados"""
        line = line.strip().replace('"', '')
        
        # Mapeamento de campos para processamento
        field_mapping = {
            "x_point:": ("x_point", lambda x: float(x)),
            "y_point:": ("y_point", lambda x: float(x)),
            "move_speed:": ("move_speed", lambda x: float(x.replace("cm/s", ""))),
            "heart_rate:": ("heart_rate", lambda x: float(x) if x != 'null' else None),
            "breath_rate:": ("breath_rate", lambda x: float(x) if x != 'null' else None)
        }
        
        for key, (field, processor) in field_mapping.items():
            if key in line:
                try:
                    value = line.split(":")[1].strip()
                    self.current_target[field] = processor(value)
                    if key == "breath_rate:":  # Último campo do conjunto
                        self.finalize_target()
                except ValueError as e:
                    logging.error(f"Erro ao processar {field}: {e}")
                    
    def finalize_target(self) -> None:
        """Finaliza o processamento do alvo atual"""
        self.current_target["timestamp"] = datetime.now().isoformat()
        
        # Cálculo de engajamento
        if self.current_target["move_speed"] == 0:
            self.current_zero_streak += 1
        else:
            self.total_engagement_seconds += self.current_zero_streak // 5
            self.current_zero_streak = 0
            
        self.matrix.append(self.current_target)
        self.interactions.append(self.current_target.copy())
        self.current_target = {}
        
    def calculate_metrics(self) -> Dict:
        """Calcula métricas de negócio"""
        return {
            "data_processamento": datetime.now(),
            "total_pessoas_passaram": sum(1 for item in self.matrix if item["move_speed"] > 0),
            "total_interacoes": len(self.interactions),
            "satisfacao_positiva": sum(1 for item in self.interactions if item.get("heart_rate") and item["heart_rate"] <= 100),
            "satisfacao_negativa": sum(1 for item in self.interactions if item.get("heart_rate") and item["heart_rate"] > 100),
            "engajamento_segundos": self.total_engagement_seconds,
            "tempo_medio_interacao": self.total_engagement_seconds / len(self.interactions) if self.interactions else 0,
            "taxa_conversao": (sum(1 for item in self.matrix if item["move_speed"] == 0) / len(self.matrix)) * 100 if self.matrix else 0
        }

class MySQLManager:
    def __init__(self):
        self.conn = mysql.connector.connect(**db_config)
        self.cursor = self.conn.cursor()
        self._create_tables()
        
    def _create_tables(self):
        """Cria as tabelas necessárias se não existirem"""
        # Tabela de interações
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
        
        # Tabela de métricas
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS radar_metricas (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data_processamento DATETIME,
                total_pessoas_passaram INT,
                total_interacoes INT,
                satisfacao_positiva INT,
                satisfacao_negativa INT,
                engajamento_segundos INT,
                tempo_medio_interacao FLOAT,
                taxa_conversao FLOAT
            )
        """)
        self.conn.commit()
    
    def insert_interacao(self, data: Dict):
        """Insere uma nova linha na tabela de interações"""
        sql = """
            INSERT INTO radar_interacoes 
            (x_point, y_point, move_speed, heart_rate, breath_rate, timestamp, sequencia_engajamento)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            data['x_point'],
            data['y_point'],
            data['move_speed'],
            data['heart_rate'],
            data['breath_rate'],
            data['timestamp'],
            data.get('sequencia_engajamento')
        )
        self.cursor.execute(sql, values)
        self.conn.commit()
    
    def insert_metricas(self, metricas: Dict):
        """Insere uma nova linha na tabela de métricas"""
        sql = """
            INSERT INTO radar_metricas 
            (data_processamento, total_pessoas_passaram, total_interacoes,
            satisfacao_positiva, satisfacao_negativa, engajamento_segundos,
            tempo_medio_interacao, taxa_conversao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = tuple(metricas.values())
        self.cursor.execute(sql, values)
        self.conn.commit()
        
    def __del__(self):
        """Fecha as conexões ao destruir o objeto"""
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

def main():
    try:
        # Inicialização
        processor = RadarDataProcessor('teste.txt')
        processor.read_data()
        
        # Processamento dos dados
        for line in processor.raw_data.strip().split('\n'):
            processor.process_line(line)
            
        # Processamento final do engajamento
        processor.total_engagement_seconds += processor.current_zero_streak // 5
        
        # Inicializa o gerenciador MySQL
        mysql_manager = MySQLManager()
        
        # Inserção dos dados processados
        sequence_counter = 0
        current_sequence_id = 0
        
        for idx, item in enumerate(processor.interactions):
            if item["move_speed"] == 0 and (idx == 0 or processor.interactions[idx-1]["move_speed"] != 0):
                current_sequence_id += 1
                
            item['sequencia_engajamento'] = current_sequence_id if item["move_speed"] == 0 else None
            
            try:
                mysql_manager.insert_interacao(item)
            except Exception as err:
                logging.error(f"Erro ao inserir interação no MySQL: {err}")
                
        # Cálculo e inserção de métricas
        metricas = processor.calculate_metrics()
        
        try:
            mysql_manager.insert_metricas(metricas)
        except Exception as err:
            logging.error(f"Erro ao inserir métricas no MySQL: {err}")
            
        logging.info(f"""
        Processamento concluído com sucesso!
        - Engajamento total: {processor.total_engagement_seconds} segundos
        - Interações analisadas: {len(processor.interactions)} medições
        - Taxa de conversão: {metricas['taxa_conversao']:.2f}%
        - Tempo médio de interação: {metricas['tempo_medio_interacao']:.2f} segundos
        """)
        
    except Exception as e:
        logging.error(f"Erro durante o processamento: {e}")
        raise

if __name__ == "__main__":
    main()