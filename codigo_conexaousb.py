import json
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv
import traceback
import time
import numpy as np
import uuid
import serial
import threading
import re

# Configuração básica de logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('radar_serial.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('radar_serial_app')

# Carregar variáveis de ambiente
load_dotenv()

# Constante para conversão do índice Doppler para velocidade
RANGE_STEP = 2.5  # Valor do RANGE_STEP do código ESP32/Arduino

def parse_serial_data(raw_data):
    """Analisa os dados brutos da porta serial para extrair informações do radar mmWave"""
    try:
        # Padrão para extrair valores do texto recebido pela porta serial
        # Procurar no formato específico do ESP32/Arduino com mmWave
        x_pattern = r'x_point:\s*([-]?\d+\.\d+)'
        y_pattern = r'y_point:\s*([-]?\d+\.\d+)'
        dop_pattern = r'dop_index:\s*(\d+)'
        move_speed_pattern = r'move_speed:\s*([-]?\d+\.\d+)\s*cm/s'
        heart_rate_pattern = r'heart_rate:\s*([-]?\d+\.\d+)'
        breath_rate_pattern = r'breath_rate:\s*([-]?\d+\.\d+)'
        
        # Extrair valores usando expressões regulares
        x_match = re.search(x_pattern, raw_data)
        y_match = re.search(y_pattern, raw_data)
        dop_match = re.search(dop_pattern, raw_data)
        speed_match = re.search(move_speed_pattern, raw_data)
        heart_match = re.search(heart_rate_pattern, raw_data)
        breath_match = re.search(breath_rate_pattern, raw_data)
        
        if x_match and y_match:
            # Dados obrigatórios: posição x e y
            x_point = float(x_match.group(1))
            y_point = float(y_match.group(1))
            
            # Velocidade de movimento: usar diretamente ou calcular do índice Doppler
            if speed_match:
                move_speed = float(speed_match.group(1))
            elif dop_match:
                dop_index = int(dop_match.group(1))
                move_speed = dop_index * RANGE_STEP
            else:
                move_speed = 0.0
            
            # Dados de sinais vitais: usar valores recebidos ou padrões
            heart_rate = float(heart_match.group(1)) if heart_match else 75.0
            breath_rate = float(breath_match.group(1)) if breath_match else 15.0
            
            return {
                'x_point': x_point,
                'y_point': y_point,
                'move_speed': move_speed,
                'heart_rate': heart_rate,
                'breath_rate': breath_rate,
                'dop_index': int(dop_match.group(1)) if dop_match else 0
            }
        else:
            # Se não for possível extrair todos os valores necessários
            if '-----Human Detected-----' in raw_data and not ('x_point:' in raw_data):
                # Mensagem somente de detecção humana sem detalhes
                logger.info("Detecção humana sem informações detalhadas")
                return None
            elif raw_data.strip():
                # Mensagem não vazia mas sem os dados necessários
                logger.debug(f"Dados incompletos: {raw_data}")
            return None
            
    except Exception as e:
        logger.error(f"Erro ao analisar dados seriais: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def convert_radar_data(raw_data):
    """Converte dados brutos do radar para o formato do banco de dados"""
    try:
        # Verificar se já é um dicionário
        if isinstance(raw_data, dict):
            data = raw_data
        else:
            # Tentar parsear como JSON primeiro
            try:
                data = json.loads(raw_data)
            except:
                # Se não for JSON, tentar parsear como texto da serial
                data = parse_serial_data(raw_data)
                if not data:
                    return None
        
        # Garantir que todos os campos necessários estão presentes
        result = {
            'x_point': float(data.get('x_point', 0)),
            'y_point': float(data.get('y_point', 0)),
            'move_speed': float(data.get('move_speed', 0)),
            'heart_rate': float(data.get('heart_rate', 75)),
            'breath_rate': float(data.get('breath_rate', 15))
        }
        
        return result
    except Exception as e:
        logger.error(f"Erro ao converter dados do radar: {str(e)}")
        logger.error(traceback.format_exc())
        return None

class FileDataManager:
    def __init__(self, data_file='radar_data.json'):
        self.data_file = data_file
        self.data = {
            'radar_dados': [],
            'shelf_sections': []
        }
        self.load_data()

    def load_data(self):
        """Carrega dados do arquivo se ele existir"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    self.data = json.load(f)
        except Exception as e:
            logger.error(f"Erro ao carregar dados do arquivo: {str(e)}")

    def save_data(self):
        """Salva dados no arquivo"""
        try:
            with open(self.data_file, 'w') as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            logger.error(f"Erro ao salvar dados no arquivo: {str(e)}")

    def insert_radar_data(self, data):
        """Insere dados do radar no arquivo"""
        try:
            # Adicionar ID único e timestamp
            data['id'] = str(uuid.uuid4())
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Adicionar aos dados existentes
            self.data['radar_dados'].append(data)
            
            # Salvar no arquivo
            self.save_data()
            logger.info("✅ Dados salvos com sucesso no arquivo!")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao salvar dados: {str(e)}")
            return False

    def get_section_at_position(self, x, y):
        """Busca seção baseada nas coordenadas (x, y)"""
        try:
            for section in self.data['shelf_sections']:
                if (section['x_start'] <= x <= section['x_end'] and
                    section['y_start'] <= y <= section['y_end'] and
                    section.get('is_active', True)):
                    return section
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar seção: {str(e)}")
            return None

class SerialRadarManager:
    def __init__(self, port=None, baudrate=115200):
        self.port = port or self.find_serial_port()
        self.baudrate = baudrate
        self.serial_connection = None
        self.is_running = False
        self.receive_thread = None
        self.data_manager = None
        self.analytics_manager = AnalyticsManager()
        
    def find_serial_port(self):
        """Tenta encontrar a porta serial do dispositivo automaticamente"""
        import serial.tools.list_ports
        
        # Listar todas as portas seriais disponíveis
        ports = list(serial.tools.list_ports.comports())
        if not ports:
            logger.error("Nenhuma porta serial encontrada!")
            return None
            
        # Procurar por portas que pareçam ser dispositivos ESP32, Arduino ou Raspberry Pi
        for port in ports:
            # Verificar descritores comuns
            desc_lower = port.description.lower()
            if any(term in desc_lower for term in 
                  ['usb', 'serial', 'uart', 'cp210', 'ch340', 'ft232', 'arduino', 
                   'esp32', 'raspberry', 'rpi', 'ttyacm', 'ttyusb']):
                logger.info(f"Porta serial encontrada: {port.device} ({port.description})")
                return port.device
                
        # Se não encontrou nenhuma específica, usar a primeira da lista
        logger.info(f"Usando primeira porta serial disponível: {ports[0].device}")
        return ports[0].device
    
    def connect(self):
        """Estabelece conexão com a porta serial"""
        if not self.port:
            logger.error("Porta serial não especificada!")
            return False
            
        try:
            logger.info(f"Conectando à porta serial {self.port} (baudrate: {self.baudrate})...")
            self.serial_connection = serial.Serial(self.port, self.baudrate, timeout=1)
            # Pequeno delay para garantir que a conexão esteja estabilizada
            time.sleep(2)
            logger.info(f"✅ Conexão serial estabelecida com sucesso!")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao conectar à porta serial: {str(e)}")
            logger.error(traceback.format_exc())
            return False
            
    def start(self, data_manager):
        """Inicia o receptor de dados seriais em uma thread separada"""
        self.data_manager = data_manager
        
        if not self.connect():
            return False
            
        self.is_running = True
        self.receive_thread = threading.Thread(target=self.receive_data_loop)
        self.receive_thread.daemon = True
        self.receive_thread.start()
        
        logger.info("Receptor de dados seriais iniciado!")
        return True
        
    def stop(self):
        """Para o receptor de dados seriais"""
        self.is_running = False
        
        if self.serial_connection:
            try:
                self.serial_connection.close()
            except:
                pass
                
        if self.receive_thread and self.receive_thread.is_alive():
            self.receive_thread.join(timeout=2)
            
        logger.info("Receptor de dados seriais parado!")
        
    def receive_data_loop(self):
        """Loop principal para receber e processar dados da porta serial"""
        buffer = ""
        message_mode = False
        message_buffer = ""
        target_data_complete = False
        
        while self.is_running:
            try:
                if not self.serial_connection.is_open:
                    self.connect()
                    time.sleep(1)
                    continue
                    
                # Ler dados disponíveis
                data = self.serial_connection.read(self.serial_connection.in_waiting or 1)
                if data:
                    text = data.decode('utf-8', errors='ignore')
                    buffer += text
                    
                    # Verificar se temos linhas completas
                    if '\n' in buffer:
                        lines = buffer.split('\n')
                        buffer = lines[-1]  # Manter o que sobrar após o último newline
                        
                        # Processar linhas completas
                        for line in lines[:-1]:
                            line = line.strip()
                            
                            # Início de uma mensagem de detecção
                            if '-----Human Detected-----' in line:
                                message_mode = True
                                message_buffer = line + '\n'
                                target_data_complete = False
                            # Continuação da mensagem de detecção
                            elif message_mode:
                                message_buffer += line + '\n'
                                
                                # Verificar se a mensagem está completa
                                if ('breath_rate:' in line or 'breath_rate:' in message_buffer):
                                    target_data_complete = True
                                
                                # Verificar se temos outra detecção (novo Target) - fim da anterior
                                if target_data_complete and (line.strip() == '' or 'Target' in line and line.startswith('Target')):
                                    # Processar os dados coletados
                                    self.process_radar_data(message_buffer)
                                    
                                    # Se for uma linha em branco, finalizar a mensagem
                                    if line.strip() == '':
                                        message_mode = False
                                        message_buffer = ""
                                        target_data_complete = False
                                    # Se for outro Target, começar novo ciclo mas manter o modo
                                    else:
                                        message_buffer = line + '\n'
                                        target_data_complete = False
                            # Outras mensagens não relacionadas à detecção
                            elif line:
                                logger.debug(f"Mensagem: {line}")
                                
                # Pequena pausa para evitar consumo excessivo de CPU
                time.sleep(0.01)
                
            except Exception as e:
                logger.error(f"❌ Erro no loop de recepção: {str(e)}")
                logger.error(traceback.format_exc())
                time.sleep(1)  # Pausa para evitar spam de logs em caso de erro
                
    def process_radar_data(self, raw_data):
        """Processa dados brutos do radar recebidos pela serial"""
        logger.info("="*50)
        logger.info("📡 Dados recebidos pela porta serial")
        logger.info(f"Dados brutos: {raw_data[:100]}...")  # Mostrar apenas o início para não poluir o log
        
        # Converter dados
        converted_data = convert_radar_data(raw_data)
        if not converted_data:
            logger.warning("⚠️ Não foi possível extrair dados do radar desta mensagem")
            return
            
        # Adicionar timestamp
        current_time = datetime.now()
        converted_data['timestamp'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Identificar seção baseado na posição
        section = self.data_manager.get_section_at_position(
            converted_data['x_point'],
            converted_data['y_point']
        )
        
        if section:
            converted_data['section_id'] = section['id']
            converted_data['product_id'] = section['product_id']
            logger.info(f"📍 Seção detectada: {section['section_name']} (Produto: {section['product_id']})")
        else:
            converted_data['section_id'] = None
            converted_data['product_id'] = None
            logger.info("❌ Nenhuma seção detectada para esta posição")
            
        # Calcular satisfação
        satisfaction_data = self.analytics_manager.calculate_satisfaction_score(
            converted_data.get('move_speed'),
            converted_data.get('heart_rate'),
            converted_data.get('breath_rate')
        )
        
        converted_data['satisfaction_score'] = satisfaction_data[0]
        converted_data['satisfaction_class'] = satisfaction_data[1]
        
        # Calcular engajamento
        is_engaged = converted_data.get('move_speed', float('inf')) <= self.analytics_manager.MOVEMENT_THRESHOLD
        converted_data['is_engaged'] = is_engaged
        
        # Log dos dados calculados
        logger.info(f"Dados processados: {converted_data}")
        logger.info(f"Dados de engajamento: engajado={is_engaged}")
        logger.info(f"Dados de satisfação: score={satisfaction_data[0]}, class={satisfaction_data[1]}")
        
        # Salvar dados no arquivo
        if self.data_manager:
            success = self.data_manager.insert_radar_data(converted_data)
            if not success:
                logger.error("❌ Falha ao salvar dados no arquivo")
        else:
            logger.warning("⚠️ Gerenciador de dados não disponível, dados não foram salvos")

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
        self.previous_heart_rates = []

    def calculate_satisfaction_score(self, move_speed, heart_rate, breath_rate):
        """
        Calcula o score de satisfação baseado nas métricas do radar e VFC
        Retorna: (score, classificação)
        """
        try:
            # Adicionar ao histórico de batimentos cardíacos
            if heart_rate:
                self.previous_heart_rates.append(heart_rate)
                # Manter apenas os últimos 10 valores
                if len(self.previous_heart_rates) > 10:
                    self.previous_heart_rates.pop(0)
            
            # Calcular VFC se houver histórico de batimentos cardíacos
            vfc_score = 0
            if len(self.previous_heart_rates) > 1:
                # Calcular a variabilidade entre os últimos batimentos
                vfc = np.std(self.previous_heart_rates[-5:])  # Usar últimos 5 valores
                vfc_score = min(1.0, vfc / 10.0)  # Normalizar VFC (assumindo variação máxima de 10 bpm)
            
            # Normalizar as métricas para uma escala de 0-1
            move_speed_norm = min(1.0, move_speed / 30.0)  # Velocidade máxima considerada: 30 cm/s
            heart_rate_norm = max(0.0, min(1.0, (heart_rate - 50) / 50))  # Faixa mais ampla: 50-100 bpm
            breath_rate_norm = max(0.0, min(1.0, (breath_rate - 10) / 15))  # Faixa mais ampla: 10-25 rpm
            
            # Pesos atualizados baseados no estudo
            WEIGHTS = {
                'move_speed': 0.3,     # Velocidade tem peso médio
                'heart_rate': 0.3,     # Frequência cardíaca tem peso médio
                'breath_rate': 0.2,    # Respiração tem peso menor
                'vfc': 0.2             # VFC tem peso significativo
            }
            
            # Calcular score ponderado (0-100)
            score = 100 * (
                WEIGHTS['move_speed'] * (1 - move_speed_norm) +  # Menor velocidade = maior satisfação
                WEIGHTS['heart_rate'] * (1 - heart_rate_norm) +  # Menor freq cardíaca = maior satisfação
                WEIGHTS['breath_rate'] * (1 - breath_rate_norm) +  # Menor freq respiratória = maior satisfação
                WEIGHTS['vfc'] * vfc_score  # Maior VFC = maior satisfação
            )
            
            # Classificar o score com níveis mais granulares
            if score >= 85:
                satisfaction_class = 'Muito Satisfeito'
            elif score >= 70:
                satisfaction_class = 'Satisfeito'
            elif score >= 55:
                satisfaction_class = 'Levemente Satisfeito'
            elif score >= 40:
                satisfaction_class = 'Neutro'
            elif score >= 25:
                satisfaction_class = 'Levemente Insatisfeito'
            elif score >= 10:
                satisfaction_class = 'Insatisfeito'
            else:
                satisfaction_class = 'Muito Insatisfeito'
                
            return score, satisfaction_class
            
        except Exception as e:
            logger.error(f"❌ Erro ao calcular satisfação: {str(e)}")
            logger.error(traceback.format_exc())
            return 50, 'Neutro'  # Valor padrão em caso de erro

def main():
    # Inicializar gerenciador de dados em arquivo
    logger.info("Iniciando FileDataManager...")
    try:
        data_manager = FileDataManager()
        logger.info("✅ FileDataManager iniciado com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro ao criar instância do FileDataManager: {e}")
        logger.error(traceback.format_exc())
        return

    # Inicializar gerenciador de radar serial
    port = os.getenv("SERIAL_PORT")  # Obter da variável de ambiente ou buscar automaticamente
    baudrate = int(os.getenv("SERIAL_BAUDRATE", "115200"))
    
    radar_manager = SerialRadarManager(port, baudrate)
    
    try:
        # Iniciar o receptor
        logger.info(f"Iniciando SerialRadarManager...")
        success = radar_manager.start(data_manager)
        
        if not success:
            logger.error("❌ Falha ao iniciar o gerenciador de radar serial")
            return
            
        logger.info("="*50)
        logger.info("🚀 Sistema Radar Serial iniciado com sucesso!")
        logger.info(f"📡 Porta serial: {radar_manager.port}")
        logger.info(f"📡 Baudrate: {radar_manager.baudrate}")
        logger.info("⚡ Pressione Ctrl+C para encerrar")
        logger.info("="*50)
        
        # Manter o programa rodando
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Encerrando por interrupção do usuário...")
        
    finally:
        # Parar o receptor
        radar_manager.stop()
        logger.info("Sistema encerrado!")

if __name__ == "__main__":
    main() 
