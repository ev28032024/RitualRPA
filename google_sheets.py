"""
Google Sheets Reader
Чтение данных аккаунтов из Google таблицы

Поддерживает два режима:
1. Публичная таблица - без авторизации (таблица должна быть открыта по ссылке)
2. Service Account - для приватных таблиц (требуется файл credentials.json)
"""
import csv
import io
import json
import os
import re
from typing import List, Dict, Any, Optional, Tuple, Union

import requests

from logger_config import get_logger

logger = get_logger("GoogleSheets")

# Опциональные зависимости для Service Account
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_API_AVAILABLE = True
except ImportError:
    GOOGLE_API_AVAILABLE = False
    service_account = None
    build = None
    HttpError = Exception


# ============================================================================
# CONSTANTS
# ============================================================================

REQUIRED_COLUMNS = ["name", "adspower_id", "discord_username"]

COLUMN_ALIASES = {
    "name": ["name", "account", "account_name", "имя", "аккаунт", "название"],
    "adspower_id": ["adspower_id", "adspower", "profile_id", "profile", "id", "профиль"],
    "discord_username": ["discord_username", "discord", "username", "user", "дискорд", "ник"]
}


# ============================================================================
# HELPERS
# ============================================================================

def _normalize_column_name(name: str) -> Optional[str]:
    """Нормализовать название колонки к стандартному формату."""
    name_lower = name.lower().strip()
    
    for standard_name, aliases in COLUMN_ALIASES.items():
        if name_lower in aliases:
            return standard_name
    
    return None


def _map_columns(header: List[str]) -> Dict[str, int]:
    """Создать маппинг колонок: стандартное_имя -> индекс."""
    column_map = {}
    
    for idx, col_name in enumerate(header):
        normalized = _normalize_column_name(col_name)
        if normalized and normalized not in column_map:
            column_map[normalized] = idx
    
    missing = [col for col in REQUIRED_COLUMNS if col not in column_map]
    if missing:
        raise ValueError(
            f"Не найдены обязательные колонки: {', '.join(missing)}\n"
            f"Найденные колонки: {', '.join(header)}\n"
            f"Ожидаемые колонки: name, adspower_id, discord_username"
        )
    
    return column_map


def _extract_cell_value(row: List[str], column_map: Dict[str, int], key: str) -> str:
    """Безопасно извлечь значение ячейки."""
    idx = column_map.get(key, -1)
    if idx >= 0 and idx < len(row):
        return str(row[idx]).strip()
    return ""


def _parse_rows_to_accounts(rows: List[List[str]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Преобразовать строки таблицы в список аккаунтов.
    
    Args:
        rows: Список строк (первая строка - заголовки)
        
    Returns:
        (список аккаунтов, список предупреждений)
    """
    if not rows:
        raise ValueError("Таблица пуста")
    
    header = rows[0]
    column_map = _map_columns(header)
    
    accounts = []
    warnings = []
    
    for row_num, row in enumerate(rows[1:], start=2):
        # Пропускаем пустые строки
        if not any(str(cell).strip() for cell in row):
            continue
        
        # Извлекаем данные
        name = _extract_cell_value(row, column_map, "name")
        adspower_id = _extract_cell_value(row, column_map, "adspower_id")
        discord_username = _extract_cell_value(row, column_map, "discord_username")
        
        # Валидация
        if not name:
            warnings.append(f"Строка {row_num}: пустое имя аккаунта, пропущено")
            continue
        
        if not adspower_id:
            warnings.append(f"Строка {row_num} ({name}): пустой adspower_id, пропущено")
            continue
        
        if not discord_username:
            warnings.append(f"Строка {row_num} ({name}): пустой discord_username, пропущено")
            continue
        
        # Удаляем @ если есть
        if discord_username.startswith("@"):
            discord_username = discord_username[1:]
        
        accounts.append({
            "name": name,
            "adspower_id": str(adspower_id),
            "discord_username": discord_username
        })
    
    if not accounts:
        raise ValueError("Не найдено ни одного валидного аккаунта в таблице")
    
    return accounts, warnings


def _extract_spreadsheet_id(url: str) -> Optional[str]:
    """
    Извлечь ID таблицы из различных форматов URL.
    
    Поддерживаемые форматы:
    - https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit
    - https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit#gid=0
    - SPREADSHEET_ID (просто ID)
    """
    # Если это просто ID (без URL)
    if not url.startswith("http"):
        if re.match(r'^[\w-]+$', url) and len(url) > 20:
            return url
        return None
    
    # Парсим URL
    patterns = [
        r'/spreadsheets/d/([a-zA-Z0-9_-]+)',
        r'spreadsheets/d/([a-zA-Z0-9_-]+)',
        r'/d/([a-zA-Z0-9_-]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None


def _extract_gid_from_url(url: str) -> Optional[int]:
    """Извлечь gid (ID листа) из URL если есть."""
    try:
        match = re.search(r'[#&?]gid=(\d+)', url)
        if match:
            return int(match.group(1))
    except (ValueError, AttributeError):
        pass
    return None


# ============================================================================
# PUBLIC ACCESS READER
# ============================================================================

class GoogleSheetsReader:
    """
    Читает данные из публичной Google таблицы (без авторизации).
    
    Таблица должна быть доступна по ссылке "Все у кого есть ссылка".
    """
    
    def __init__(
        self, 
        url: str, 
        sheet_name: Optional[str] = None, 
        sheet_gid: Optional[int] = None
    ):
        """
        Args:
            url: URL Google таблицы
            sheet_name: Название листа (не используется для публичного доступа)
            sheet_gid: ID листа (gid параметр)
        """
        self.original_url = url
        self.sheet_name = sheet_name
        self.spreadsheet_id = _extract_spreadsheet_id(url)
        
        if not self.spreadsheet_id:
            raise ValueError(f"Не удалось извлечь ID таблицы из URL: {url}")
        
        self.sheet_gid = sheet_gid if sheet_gid is not None else _extract_gid_from_url(url)
    
    def _build_csv_url(self) -> str:
        """Построить URL для экспорта в CSV."""
        base_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export"
        params = ["format=csv"]
        
        if self.sheet_gid is not None:
            params.append(f"gid={self.sheet_gid}")
        
        return f"{base_url}?{'&'.join(params)}"
    
    def fetch_accounts(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Загрузить аккаунты из Google таблицы.
        
        Returns:
            (список аккаунтов, список предупреждений)
        """
        csv_url = self._build_csv_url()
        logger.info("Загрузка данных из Google Sheets (публичный доступ)...")
        logger.debug(f"URL: {csv_url}")
        
        try:
            response = requests.get(csv_url, timeout=30)
            response.raise_for_status()
            
            # Проверяем что получили CSV, а не HTML
            content_type = response.headers.get('content-type', '')
            if 'text/html' in content_type:
                raise ValueError(
                    "Таблица недоступна. Убедитесь что:\n"
                    "1. Таблица существует\n"
                    "2. Доступ открыт: Файл → Поделиться → 'Все у кого есть ссылка'\n"
                    "   Или используйте Service Account для приватных таблиц"
                )
            
            content = response.content.decode('utf-8-sig')
            reader = csv.reader(io.StringIO(content))
            rows = list(reader)
            
            accounts, warnings = _parse_rows_to_accounts(rows)
            logger.info(f"✅ Загружено {len(accounts)} аккаунтов из Google Sheets")
            
            return accounts, warnings
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Ошибка подключения к Google Sheets: {e}")
        except csv.Error as e:
            raise ValueError(f"Ошибка парсинга CSV: {e}")
    
    def test_connection(self) -> bool:
        """Проверить доступность таблицы."""
        try:
            csv_url = self._build_csv_url()
            response = requests.head(csv_url, timeout=10, allow_redirects=True)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False


# ============================================================================
# SERVICE ACCOUNT READER
# ============================================================================

class GoogleSheetsServiceAccount:
    """
    Читает данные из Google таблицы через Service Account.
    
    Требуется:
    1. Создать проект в Google Cloud Console
    2. Включить Google Sheets API
    3. Создать Service Account и скачать credentials.json
    4. Предоставить доступ к таблице email'у сервисного аккаунта
    """
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    
    def __init__(
        self, 
        url: str, 
        credentials_path: str,
        sheet_name: Optional[str] = None, 
        sheet_gid: Optional[int] = None
    ):
        """
        Args:
            url: URL или ID Google таблицы
            credentials_path: Путь к файлу credentials.json
            sheet_name: Название листа
            sheet_gid: ID листа
        """
        if not GOOGLE_API_AVAILABLE:
            raise ImportError(
                "Для использования Service Account установите зависимости:\n"
                "pip install google-auth google-api-python-client"
            )
        
        self.spreadsheet_id = _extract_spreadsheet_id(url)
        if not self.spreadsheet_id:
            raise ValueError(f"Не удалось извлечь ID таблицы из URL: {url}")
        
        self.credentials_path = credentials_path
        self.sheet_name = sheet_name
        self.sheet_gid = sheet_gid if sheet_gid is not None else _extract_gid_from_url(url)
        
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(
                f"Файл credentials не найден: {credentials_path}\n"
                "Скачайте его из Google Cloud Console → IAM → Service Accounts"
            )
        
        self._service = None
    
    def _get_service(self):
        """Получить авторизованный сервис Google Sheets API."""
        if self._service is None:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=self.SCOPES
            )
            self._service = build('sheets', 'v4', credentials=credentials)
        return self._service
    
    def _get_sheet_title_by_gid(self, gid: int) -> Optional[str]:
        """Получить название листа по его gid."""
        try:
            service = self._get_service()
            spreadsheet = service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            for sheet in spreadsheet.get('sheets', []):
                props = sheet.get('properties', {})
                if props.get('sheetId') == gid:
                    return props.get('title')
        except Exception as e:
            logger.debug(f"Could not get sheet title by gid: {e}")
        return None
    
    def _determine_range_name(self) -> str:
        """Определить диапазон для запроса."""
        if self.sheet_name:
            return f"'{self.sheet_name}'"
        
        if self.sheet_gid is not None:
            sheet_title = self._get_sheet_title_by_gid(self.sheet_gid)
            if sheet_title:
                return f"'{sheet_title}'"
        
        return "Sheet1"  # Первый лист по умолчанию
    
    def fetch_accounts(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Загрузить аккаунты из Google таблицы через API."""
        logger.info("Загрузка данных из Google Sheets (Service Account)...")
        
        try:
            service = self._get_service()
            range_name = self._determine_range_name()
            
            result = service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            
            rows = result.get('values', [])
            
            if not rows:
                raise ValueError("Таблица пуста или лист не найден")
            
            accounts, warnings = _parse_rows_to_accounts(rows)
            logger.info(f"✅ Загружено {len(accounts)} аккаунтов из Google Sheets")
            
            return accounts, warnings
            
        except HttpError as e:
            if e.resp.status == 403:
                raise PermissionError(
                    "Нет доступа к таблице. Убедитесь что:\n"
                    "1. Google Sheets API включен в проекте\n"
                    "2. Email сервисного аккаунта добавлен в доступ к таблице"
                )
            elif e.resp.status == 404:
                raise ValueError(f"Таблица не найдена: {self.spreadsheet_id}")
            else:
                raise ConnectionError(f"Ошибка Google Sheets API: {e}")
    
    def test_connection(self) -> bool:
        """Проверить доступность таблицы."""
        try:
            service = self._get_service()
            service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            return True
        except Exception:
            return False
    
    def get_service_account_email(self) -> Optional[str]:
        """Получить email сервисного аккаунта из credentials."""
        try:
            with open(self.credentials_path, 'r') as f:
                creds = json.load(f)
            return creds.get('client_email')
        except (IOError, json.JSONDecodeError, KeyError):
            return None


# ============================================================================
# FACTORY FUNCTIONS
# ============================================================================

ReaderType = Union[GoogleSheetsReader, GoogleSheetsServiceAccount]


def create_reader(
    url: str,
    credentials_path: Optional[str] = None,
    sheet_name: Optional[str] = None,
    sheet_gid: Optional[int] = None
) -> ReaderType:
    """
    Создать подходящий reader на основе параметров.
    
    Returns:
        GoogleSheetsReader или GoogleSheetsServiceAccount
    """
    if credentials_path:
        return GoogleSheetsServiceAccount(
            url, 
            credentials_path, 
            sheet_name=sheet_name, 
            sheet_gid=sheet_gid
        )
    return GoogleSheetsReader(url, sheet_name, sheet_gid)


def load_accounts_from_sheets(
    url: str, 
    credentials_path: Optional[str] = None,
    sheet_name: Optional[str] = None,
    sheet_gid: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Универсальная функция для загрузки аккаунтов из Google Sheets.
    
    Автоматически выбирает метод:
    - Если указан credentials_path → Service Account
    - Иначе → публичный доступ
    """
    reader = create_reader(url, credentials_path, sheet_name, sheet_gid)
    accounts, warnings = reader.fetch_accounts()
    
    for warning in warnings:
        logger.warning(warning)
    
    return accounts


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Тестирование подключения к Google Sheets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  # Публичная таблица
  python google_sheets.py "https://docs.google.com/spreadsheets/d/XXX/edit"
  
  # Приватная таблица через Service Account
  python google_sheets.py "https://docs.google.com/spreadsheets/d/XXX/edit" -c credentials.json
  
  # Конкретный лист
  python google_sheets.py "URL" --gid 123456789
        """
    )
    
    parser.add_argument("url", help="URL или ID Google таблицы")
    parser.add_argument("-c", "--credentials", help="Путь к credentials.json")
    parser.add_argument("--gid", type=int, help="ID листа (gid)")
    parser.add_argument("--sheet", help="Название листа")
    
    args = parser.parse_args()
    
    print(f"📊 Тестирование Google Sheets Reader")
    print(f"URL: {args.url}")
    
    mode = "Service Account" if args.credentials else "Публичный доступ"
    print(f"Режим: {mode}")
    
    if args.credentials:
        print(f"Credentials: {args.credentials}")
    
    print()
    
    try:
        reader = create_reader(
            args.url,
            credentials_path=args.credentials,
            sheet_name=args.sheet,
            sheet_gid=args.gid
        )
        
        print(f"Spreadsheet ID: {reader.spreadsheet_id}")
        
        if hasattr(reader, 'sheet_gid') and reader.sheet_gid is not None:
            print(f"Sheet GID: {reader.sheet_gid}")
        
        if isinstance(reader, GoogleSheetsServiceAccount):
            email = reader.get_service_account_email()
            if email:
                print(f"Service Account Email: {email}")
                print(f"\n💡 Добавьте этот email в доступ к таблице!")
        
        print()
        
        accounts, warnings = reader.fetch_accounts()
        
        print(f"✅ Найдено аккаунтов: {len(accounts)}\n")
        
        for i, acc in enumerate(accounts, 1):
            print(f"{i}. {acc['name']}")
            print(f"   AdsPower ID: {acc['adspower_id']}")
            print(f"   Discord: @{acc['discord_username']}")
        
        if warnings:
            print(f"\n⚠️ Предупреждения:")
            for w in warnings:
                print(f"   {w}")
                
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)
