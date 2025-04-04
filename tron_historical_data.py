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
        
        # Создаем таблицу для хранения курсов валют относительно USDT
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tron_currency_rates (
                timestamp INTEGER,
                date TEXT,
                symbol TEXT,
                price_usdt REAL,
                volume_24h REAL,
                change_24h REAL,
                PRIMARY KEY (date, symbol)
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

def get_historical_transactions(hours=None, days=None):
    """Получает исторические транзакции за указанное количество часов или дней"""
    end_time = int(time.time() * 1000)  # Текущее время в миллисекундах
    
    if hours is not None:
        # Рассчитываем временной интервал в часах
        start_time = end_time - (hours * 60 * 60 * 1000)
        time_desc = f"{hours} час(ов)"
    elif days is not None:
        # Рассчитываем временной интервал в днях
        start_time = end_time - (days * 24 * 60 * 60 * 1000)
        time_desc = f"{days} дней"
    else:
        # По умолчанию - 1 день
        start_time = end_time - (24 * 60 * 60 * 1000)
        time_desc = "1 день"
    
    all_transactions = []
    total_count = 0
    page_size = 50
    page = 0
    
    # Получаем общее количество транзакций
    _, total_tx = get_transactions(start_time, end_time, 1, 0)
    
    if total_tx == 0:
        logger.info(f"За последние {time_desc} транзакций не найдено")
        return all_transactions
    
    logger.info(f"Всего найдено {total_tx} транзакций за последние {time_desc}")
    
    # Получаем транзакции постранично
    while True:
        transactions, _ = get_transactions(start_time, end_time, page_size, page * page_size)
        if not transactions:
            break
        
        # Обрабатываем и добавляем данные
        for tx in transactions:
            tx_data = extract_transaction_data(tx)
            all_transactions.append(tx_data)
        
        total_count += len(transactions)
        logger.info(f"Получено {len(transactions)} транзакций (страница {page+1}), всего: {total_count}")
        
        if len(transactions) < page_size or total_count >= total_tx:
            break
        
        page += 1
        time.sleep(0.5)  # Небольшая задержка, чтобы не перегружать API
    
    logger.info(f"Всего получено {total_count} транзакций за последние {time_desc}")
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

def get_currency_rates():
    """Получает текущие курсы валют относительно USDT"""
    url = "https://apilist.tronscanapi.com/api/token/price"
    
    data = make_api_request(url)
    if not data or "data" not in data:
        logger.error("Не удалось получить данные о курсах валют")
        return []
    
    currency_rates = []
    current_time = int(time.time() * 1000)
    current_date = datetime.fromtimestamp(current_time / 1000).strftime("%Y-%m-%d")
    
    # Обрабатываем курсы валют
    for rate_data in data["data"]:
        symbol = rate_data.get("symbol", "")
        if not symbol:
            continue
        
        # Собираем данные о курсе
        currency_rate = {
            "timestamp": current_time,
            "date": current_date,
            "symbol": symbol,
            "price_usdt": float(rate_data.get("priceInUsd", 0)),
            "volume_24h": float(rate_data.get("volume24h", 0)),
            "change_24h": float(rate_data.get("percentChange24h", 0))
        }
        
        currency_rates.append(currency_rate)
    
    logger.info(f"Получено {len(currency_rates)} курсов валют")
    return currency_rates

def save_currency_rates_to_sqlite(currency_rates):
    """Сохраняет курсы валют в SQLite"""
    if not currency_rates:
        logger.warning("Нет данных о курсах валют для сохранения")
        return 0
    
    conn = None
    success_count = 0
    
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        for rate in currency_rates:
            cursor.execute('''
                INSERT OR REPLACE INTO tron_currency_rates
                (timestamp, date, symbol, price_usdt, volume_24h, change_24h)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                rate["timestamp"],
                rate["date"],
                rate["symbol"],
                rate["price_usdt"],
                rate["volume_24h"],
                rate["change_24h"]
            ))
            success_count += 1
        
        conn.commit()
        logger.info(f"Сохранено {success_count} курсов валют в SQLite")
        return success_count
    except sqlite3.Error as e:
        logger.error(f"Ошибка сохранения курсов валют в SQLite: {e}")
        return 0
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
    parser.add_argument("--currency-rates", action="store_true", help="Получить текущие курсы валют")
    
    args = parser.parse_args()
    
    # Инициализируем базу данных SQLite
    if not initialize_sqlite_db():
        logger.error("Не удалось инициализировать базу данных SQLite. Завершаем работу.")
        sys.exit(1)
    
    # Получаем курсы валют, если запрошено
    if args.currency_rates:
        currency_rates = get_currency_rates()
        save_currency_rates_to_sqlite(currency_rates)
    
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