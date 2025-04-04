import os
import sys
import requests
import json
import time
import datetime
import logging
import argparse
import sqlite3
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Адрес TRON кошелька
TRON_WALLET_ADDRESS = os.getenv('TRON_WALLET_ADDRESS', 'TRaH4GZtNiTXfooiufvYyVhAY7fuhUrs2v')

# Путь к SQLite базе данных
SQLITE_DB_PATH = os.getenv('SQLITE_DB_PATH', 'tron_data.db')

# Настройка соединения с API Tronscan
TRONSCAN_API_URL = "https://apilist.tronscanapi.com/api"

# Максимальное количество попыток для API запросов
MAX_RETRIES = 3
RETRY_DELAY = 2  # секунды

def initialize_sqlite_db():
    """Инициализирует базу данных SQLite и создает необходимые таблицы"""
    conn = None
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        # Создаем таблицу для хранения транзакций
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tron_transactions (
                hash TEXT PRIMARY KEY,
                wallet TEXT,
                timestamp INTEGER,
                block INTEGER,
                contract_type INTEGER,
                status TEXT,
                amount REAL,
                fee REAL,
                net_fee REAL,
                energy_fee REAL,
                energy_usage REAL,
                energy_usage_total REAL,
                origin_energy_usage REAL,
                energy_penalty_total REAL,
                net_usage REAL,
                
                -- Детальная информация из trigger_info
                trigger_method TEXT,
                trigger_method_id TEXT,
                trigger_method_name TEXT,
                trigger_contract_address TEXT,
                trigger_parameter_to TEXT,
                trigger_parameter_value TEXT,
                
                -- Полный оригинальный JSON ответа
                orig_query_json TEXT
            )
        ''')
        
        # Создаем таблицу для сводной статистики по времени
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tron_energy_stats (
                time_period TEXT,
                wallet TEXT,
                start_timestamp INTEGER,
                end_timestamp INTEGER, 
                total_transactions INTEGER,
                successful_transactions INTEGER,
                failed_transactions INTEGER,
                total_fee REAL,
                total_net_fee REAL,
                total_energy_fee REAL,
                total_energy_usage REAL,
                total_energy_usage_total REAL,
                total_energy_penalty REAL,
                total_net_usage REAL,
                PRIMARY KEY (time_period, wallet)
            )
        ''')
        
        # Создаем таблицу для детализации по типам контрактов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tron_contract_stats (
                time_period TEXT,
                wallet TEXT,
                contract_type INTEGER,
                transaction_count INTEGER,
                total_fee REAL,
                total_energy_fee REAL,
                total_energy_usage REAL,
                PRIMARY KEY (time_period, wallet, contract_type)
            )
        ''')
        
        conn.commit()
        logger.info("База данных SQLite инициализирована")
        return True
    except sqlite3.Error as e:
        logger.error(f"Ошибка инициализации SQLite: {e}")
        return False
    finally:
        if conn:
            conn.close()

def make_api_request(url, params=None, retries=MAX_RETRIES):
    """Выполняет запрос к API с повторными попытками при неудаче"""
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.warning(f"Ошибка API запроса (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Не удалось выполнить запрос после {retries} попыток: {url}")
                return None

def get_transactions(start_timestamp, end_timestamp, limit=50, start=0):
    """Получает список транзакций для кошелька из Tronscan API"""
    url = f"{TRONSCAN_API_URL}/transaction"
    params = {
        "address": TRON_WALLET_ADDRESS,
        "start": start,
        "limit": limit,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "sort": "-timestamp",
        "count": True
    }
    
    data = make_api_request(url, params)
    if data and "data" in data:
        return data["data"], data.get("total", 0)
    return [], 0

def extract_transaction_data(tx):
    """Извлекает данные о транзакции из ответа API"""
    # Получаем базовую информацию о транзакции
    tx_data = {
        "hash": tx.get("hash", ""),
        "wallet": TRON_WALLET_ADDRESS,
        "timestamp": tx.get("timestamp", 0),
        "block": tx.get("block", 0),
        "contract_type": tx.get("contractType", 0),
        "status": "success" if tx.get("contractRet") == "SUCCESS" else "failed",
        "amount": float(tx.get("amount", 0)),
        "fee": 0,
        "net_fee": 0,
        "energy_fee": 0,
        "energy_usage": 0,
        "energy_usage_total": 0,
        "origin_energy_usage": 0,
        "energy_penalty_total": 0,
        "net_usage": 0,
        
        # Информация из trigger_info (для смарт-контрактов)
        "trigger_method": "",
        "trigger_method_id": "",
        "trigger_method_name": "",
        "trigger_contract_address": "",
        "trigger_parameter_to": "",
        "trigger_parameter_value": "",
        
        # Сохраняем оригинальный JSON
        "orig_query_json": json.dumps(tx)
    }
    
    # Извлекаем данные о затратах ресурсов из поля cost
    cost = tx.get("cost", {})
    if cost:
        tx_data.update({
            "fee": float(cost.get("fee", 0)),
            "net_fee": float(cost.get("net_fee", 0)),
            "energy_fee": float(cost.get("energy_fee", 0)),
            "energy_usage": float(cost.get("energy_usage", 0)),
            "energy_usage_total": float(cost.get("energy_usage_total", 0)),
            "origin_energy_usage": float(cost.get("origin_energy_usage", 0)),
            "energy_penalty_total": float(cost.get("energy_penalty_total", 0)),
            "net_usage": float(cost.get("net_usage", 0))
        })
    
    # Извлекаем информацию из trigger_info (для смарт-контрактов)
    trigger_info = tx.get("trigger_info", {})
    if trigger_info:
        tx_data.update({
            "trigger_method": trigger_info.get("method", ""),
            "trigger_method_id": trigger_info.get("methodId", ""),
            "trigger_method_name": trigger_info.get("methodName", ""),
            "trigger_contract_address": trigger_info.get("contract_address", ""),
        })
        
        # Извлекаем параметры (если есть)
        parameter = trigger_info.get("parameter", {})
        if parameter:
            tx_data.update({
                "trigger_parameter_to": parameter.get("_to", ""),
                "trigger_parameter_value": parameter.get("_value", "")
            })
    
    return tx_data

def save_transactions_to_sqlite(transactions):
    """Сохраняет данные транзакций в SQLite"""
    success_count = 0
    error_count = 0
    
    conn = None
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        for tx in transactions:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO tron_transactions 
                    (hash, wallet, timestamp, block, contract_type, status, amount, fee,
                    net_fee, energy_fee, energy_usage, energy_usage_total, origin_energy_usage,
                    energy_penalty_total, net_usage, 
                    trigger_method, trigger_method_id, trigger_method_name, trigger_contract_address,
                    trigger_parameter_to, trigger_parameter_value, orig_query_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    tx["hash"],
                    tx["wallet"],
                    tx["timestamp"],
                    tx["block"],
                    tx["contract_type"],
                    tx["status"],
                    tx["amount"],
                    tx["fee"],
                    tx["net_fee"],
                    tx["energy_fee"],
                    tx["energy_usage"],
                    tx["energy_usage_total"],
                    tx["origin_energy_usage"],
                    tx["energy_penalty_total"],
                    tx["net_usage"],
                    tx["trigger_method"],
                    tx["trigger_method_id"],
                    tx["trigger_method_name"],
                    tx["trigger_contract_address"],
                    tx["trigger_parameter_to"],
                    tx["trigger_parameter_value"],
                    tx["orig_query_json"]
                ))
                success_count += 1
            except sqlite3.Error as e:
                logger.error(f"Ошибка записи транзакции {tx['hash']} в SQLite: {e}")
                error_count += 1
        
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Ошибка подключения к SQLite: {e}")
        return 0, error_count + len(transactions)
    finally:
        if conn:
            conn.close()
    
    logger.info(f"Записано {success_count} транзакций в SQLite, ошибок: {error_count}")
    return success_count, error_count

def get_last_transaction_date():
    """Получает дату последней транзакции из базы данных"""
    conn = None
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        # Проверяем последнюю транзакцию
        cursor.execute('''
            SELECT MAX(timestamp) FROM tron_transactions
        ''')
        last_timestamp = cursor.fetchone()[0]
        
        return last_timestamp if last_timestamp else None
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении последней даты из базы: {e}")
        return None
    finally:
        if conn:
            conn.close()

def check_transactions_exist(start_timestamp, end_timestamp):
    """Проверяет наличие транзакций в базе данных для указанного периода"""
    conn = None
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        # Проверяем количество транзакций в периоде
        cursor.execute('''
            SELECT COUNT(*) FROM tron_transactions 
            WHERE timestamp >= ? AND timestamp <= ?
        ''', (start_timestamp, end_timestamp))
        count = cursor.fetchone()[0]
        
        return count > 0
    except sqlite3.Error as e:
        logger.error(f"Ошибка при проверке существующих транзакций: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_historical_transactions(hours=None, days=None):
    """Получает исторические транзакции за указанное количество часов или дней"""
    end_time = int(time.time() * 1000)  # Текущее время в миллисекундах
    
    if hours is not None:
        # Рассчитываем временной интервал в часах
        total_interval = hours * 60 * 60 * 1000
        time_desc = f"{hours} час(ов)"
    elif days is not None:
        # Рассчитываем временной интервал в днях
        total_interval = days * 24 * 60 * 60 * 1000
        time_desc = f"{days} дней"
    else:
        # По умолчанию - 1 день
        total_interval = 24 * 60 * 60 * 1000
        time_desc = "1 день"
    
    # Проверяем последнюю дату в базе
    last_timestamp = get_last_transaction_date()
    if last_timestamp:
        logger.info(f"Найдены существующие данные до {datetime.fromtimestamp(last_timestamp/1000)}")
        # Начинаем с последней известной даты
        end_time = last_timestamp
    
    start_time = end_time - total_interval
    original_start_time = start_time
    
    # Размер одного временного окна (7 дней)
    window_size = 7 * 24 * 60 * 60 * 1000
    
    all_transactions = []
    current_window_end = end_time
    
    while current_window_end > original_start_time:
        current_window_start = max(original_start_time, current_window_end - window_size)
        
        # Проверяем, есть ли уже данные за этот период
        if check_transactions_exist(current_window_start, current_window_end):
            logger.info(f"Пропуск периода {datetime.fromtimestamp(current_window_start/1000)} - {datetime.fromtimestamp(current_window_end/1000)}: данные уже существуют")
            current_window_end = current_window_start
            continue
        
        logger.info(f"Обработка периода: {datetime.fromtimestamp(current_window_start/1000)} - {datetime.fromtimestamp(current_window_end/1000)}")
        
        # Получаем транзакции для текущего окна
        window_transactions = []
        page = 0
        page_size = 50
        
        while True:
            transactions, total_tx = get_transactions(current_window_start, current_window_end, page_size, page * page_size)
            if not transactions:
                break
            
            # Обрабатываем и добавляем данные
            for tx in transactions:
                tx_data = extract_transaction_data(tx)
                window_transactions.append(tx_data)
            
            logger.info(f"Получено {len(transactions)} транзакций (страница {page+1}), всего в текущем окне: {len(window_transactions)}")
            
            if len(transactions) < page_size:
                break
                
            page += 1
            time.sleep(0.5)  # Небольшая задержка между запросами
        
        # Добавляем транзакции текущего окна к общему списку
        if window_transactions:
            all_transactions.extend(window_transactions)
            logger.info(f"В текущем окне найдено {len(window_transactions)} транзакций")
        else:
            logger.info("В текущем окне транзакций не найдено")
        
        # Сдвигаем окно назад во времени
        current_window_end = current_window_start
        
        # Если в текущем окне не было транзакций, возможно есть смысл увеличить размер окна
        if not window_transactions:
            window_size = min(window_size * 2, total_interval)  # Увеличиваем окно, но не больше общего интервала
    
    logger.info(f"Всего получено {len(all_transactions)} новых транзакций за последние {time_desc}")
    return all_transactions

def create_time_period_key(timestamp, period_type):
    """Создает ключ временного периода на основе типа периода"""
    dt = datetime.fromtimestamp(timestamp / 1000)
    
    if period_type == 'hour':
        return dt.strftime("%Y-%m-%d_%H")
    elif period_type == 'day':
        return dt.strftime("%Y-%m-%d")
    elif period_type == 'month':
        return dt.strftime("%Y-%m")
    else:
        return dt.strftime("%Y-%m-%d")

def generate_energy_statistics(transactions, period_type='hour'):
    """Генерирует статистику использования энергии по временным периодам"""
    periods = {}
    contract_stats = {}
    
    for tx in transactions:
        # Определяем временной период
        time_key = create_time_period_key(tx["timestamp"], period_type)
        
        # Инициализируем данные для периода, если они ещё не существуют
        if time_key not in periods:
            periods[time_key] = {
                "time_period": time_key,
                "wallet": TRON_WALLET_ADDRESS,
                "start_timestamp": tx["timestamp"],  # Будет обновлено позже для первой транзакции
                "end_timestamp": tx["timestamp"],    # Будет обновлено позже для последней транзакции
                "total_transactions": 0,
                "successful_transactions": 0,
                "failed_transactions": 0,
                "total_fee": 0,
                "total_net_fee": 0,
                "total_energy_fee": 0,
                "total_energy_usage": 0,
                "total_energy_usage_total": 0, 
                "total_energy_penalty": 0,
                "total_net_usage": 0
            }
        
        # Инициализируем статистику по контрактам для этого периода
        contract_key = f"{time_key}_{tx['contract_type']}"
        if contract_key not in contract_stats:
            contract_stats[contract_key] = {
                "time_period": time_key,
                "wallet": TRON_WALLET_ADDRESS,
                "contract_type": tx["contract_type"],
                "transaction_count": 0,
                "total_fee": 0,
                "total_energy_fee": 0,
                "total_energy_usage": 0
            }
        
        # Обновляем временные метки первой и последней транзакции
        periods[time_key]["start_timestamp"] = min(periods[time_key]["start_timestamp"], tx["timestamp"])
        periods[time_key]["end_timestamp"] = max(periods[time_key]["end_timestamp"], tx["timestamp"])
        
        # Обновляем статистику периода
        periods[time_key]["total_transactions"] += 1
        
        if tx["status"] == "success":
            periods[time_key]["successful_transactions"] += 1
        else:
            periods[time_key]["failed_transactions"] += 1
        
        # Добавляем данные о потреблении ресурсов
        periods[time_key]["total_fee"] += tx["fee"]
        periods[time_key]["total_net_fee"] += tx["net_fee"]
        periods[time_key]["total_energy_fee"] += tx["energy_fee"]
        periods[time_key]["total_energy_usage"] += tx["energy_usage"]
        periods[time_key]["total_energy_usage_total"] += tx["energy_usage_total"]
        periods[time_key]["total_energy_penalty"] += tx["energy_penalty_total"]
        periods[time_key]["total_net_usage"] += tx["net_usage"]
        
        # Обновляем статистику по типам контрактов
        contract_stats[contract_key]["transaction_count"] += 1
        contract_stats[contract_key]["total_fee"] += tx["fee"]
        contract_stats[contract_key]["total_energy_fee"] += tx["energy_fee"]
        contract_stats[contract_key]["total_energy_usage"] += tx["energy_usage"]
    
    # Преобразуем словари в списки для удобства
    return list(periods.values()), list(contract_stats.values())

def save_energy_stats_to_sqlite(energy_stats, contract_stats):
    """Сохраняет статистику использования энергии в SQLite"""
    conn = None
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        # Сохраняем статистику по периодам
        for period in energy_stats:
            cursor.execute('''
                INSERT OR REPLACE INTO tron_energy_stats
                (time_period, wallet, start_timestamp, end_timestamp, total_transactions,
                successful_transactions, failed_transactions, total_fee, total_net_fee,
                total_energy_fee, total_energy_usage, total_energy_usage_total,
                total_energy_penalty, total_net_usage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                period["time_period"],
                period["wallet"],
                period["start_timestamp"],
                period["end_timestamp"],
                period["total_transactions"],
                period["successful_transactions"],
                period["failed_transactions"],
                period["total_fee"],
                period["total_net_fee"],
                period["total_energy_fee"],
                period["total_energy_usage"],
                period["total_energy_usage_total"],
                period["total_energy_penalty"],
                period["total_net_usage"]
            ))
        
        # Сохраняем статистику по контрактам
        for contract in contract_stats:
            cursor.execute('''
                INSERT OR REPLACE INTO tron_contract_stats
                (time_period, wallet, contract_type, transaction_count,
                total_fee, total_energy_fee, total_energy_usage)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                contract["time_period"],
                contract["wallet"],
                contract["contract_type"],
                contract["transaction_count"],
                contract["total_fee"],
                contract["total_energy_fee"],
                contract["total_energy_usage"]
            ))
        
        conn.commit()
        logger.info(f"Сохранена статистика для {len(energy_stats)} периодов и {len(contract_stats)} типов контрактов")
        return True
    except sqlite3.Error as e:
        logger.error(f"Ошибка сохранения статистики в SQLite: {e}")
        return False
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description="Сбор данных о транзакциях и энергии TRON кошелька")
    
    time_group = parser.add_mutually_exclusive_group()
    time_group.add_argument("--hours", type=int, help="Количество часов для сбора данных")
    time_group.add_argument("--days", type=int, help="Количество дней для сбора данных")
    
    parser.add_argument("--period", choices=["hour", "day", "month"], default="hour", 
                        help="Тип периода для агрегации статистики (по умолчанию: hour)")
    
    args = parser.parse_args()
    
    # Инициализируем базу данных SQLite
    if not initialize_sqlite_db():
        logger.error("Не удалось инициализировать базу данных SQLite. Завершаем работу.")
        sys.exit(1)
    
    # Получаем исторические транзакции
    transactions = get_historical_transactions(hours=args.hours, days=args.days)
    if not transactions:
        logger.warning("Не найдено транзакций за указанный период.")
        sys.exit(0)
    
    # Сохраняем данные о транзакциях в SQLite
    success_tx, error_tx = save_transactions_to_sqlite(transactions)
    
    # Создаем и сохраняем статистику использования энергии
    energy_stats, contract_stats = generate_energy_statistics(transactions, args.period)
    save_energy_stats_to_sqlite(energy_stats, contract_stats)
    
    # Выводим краткую сводку
    if energy_stats:
        total_tx = sum(period["total_transactions"] for period in energy_stats)
        total_energy = sum(period["total_energy_usage_total"] for period in energy_stats)
        total_energy_fee = sum(period["total_energy_fee"] for period in energy_stats)
        
        logger.info(f"Краткая сводка:")
        logger.info(f"- Всего транзакций: {total_tx}")
        logger.info(f"- Общее использование энергии: {total_energy}")
        logger.info(f"- Общая стоимость энергии: {total_energy_fee} TRX")
    
    logger.info(f"Сбор данных завершен: {success_tx} транзакций обработано")

if __name__ == "__main__":
    main()