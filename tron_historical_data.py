import os
import sys
import requests
import json
import time
import datetime
import logging
import argparse
import sqlite3
import random
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
MAX_RETRIES = 5
MIN_RETRY_DELAY = 10  # минимальная задержка в секундах
MAX_RETRY_DELAY = 30  # максимальная задержка в секундах

# Список User-Agent для ротации
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
]

def get_random_headers():
    """Генерирует случайные заголовки для HTTP-запроса"""
    user_agent = random.choice(USER_AGENTS)
    return {
        'User-Agent': user_agent,
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://tronscan.org',
        'Referer': 'https://tronscan.org/',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
    }

def create_session():
    """Создает сессию с настроенными повторными попытками и прокси"""
    session = requests.Session()
    
    # Настройка повторных попыток
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Настройка прокси из переменных окружения
    proxies = {}
    if os.getenv('HTTP_PROXY'):
        proxies['http'] = os.getenv('HTTP_PROXY')
    if os.getenv('HTTPS_PROXY'):
        proxies['https'] = os.getenv('HTTPS_PROXY')
    if proxies:
        session.proxies.update(proxies)
        logger.info(f"Используются прокси: {proxies}")
    
    return session

def make_api_request(url, params=None, retries=MAX_RETRIES):
    """Выполняет запрос к API с повторными попытками при неудаче"""
    session = create_session()
    
    for attempt in range(retries):
        try:
            # Случайная задержка между запросами
            delay = random.uniform(MIN_RETRY_DELAY, MAX_RETRY_DELAY)
            if attempt > 0:
                logger.info(f"Ожидание {delay:.2f} секунд перед повторной попыткой...")
                time.sleep(delay)
            
            headers = get_random_headers()
            logger.debug(f"Использование User-Agent: {headers['User-Agent']}")
            
            response = session.get(url, params=params, headers=headers, timeout=30)
            
            # Если получили 403, делаем более длительную паузу
            if response.status_code == 403:
                logger.warning(f"Получен код 403 (попытка {attempt+1}/{retries}). Возможно, превышен лимит запросов.")
                if attempt < retries - 1:
                    extended_delay = random.uniform(MAX_RETRY_DELAY * 2, MAX_RETRY_DELAY * 4)
                    logger.info(f"Ожидание {extended_delay:.2f} секунд перед следующей попыткой...")
                    time.sleep(extended_delay)
                continue
            
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            logger.warning(f"Ошибка API запроса (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                continue
            else:
                logger.error(f"Не удалось выполнить запрос после {retries} попыток: {url}")
                return None
        finally:
            session.close()

[Оставшаяся часть кода остается без изменений...]