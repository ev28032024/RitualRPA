"""
Account Manager
Manages account configurations and execution tracking
Supports loading accounts from config.json or Google Sheets
"""
import json
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, asdict

from .google_sheets import create_reader
from .logger_config import get_logger

logger = get_logger("AccountManager")


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Account:
    """Represents a Discord account with AdsPower profile"""
    name: str
    adspower_id: str  # Can be profile ID (string) or serial number (numeric string)
    discord_username: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Account':
        """Create Account from dictionary."""
        adspower_id = data.get("adspower_id", "")
        if isinstance(adspower_id, (int, float)):
            adspower_id = str(int(adspower_id))
        
        return cls(
            name=data.get("name", ""),
            adspower_id=str(adspower_id) if adspower_id else "",
            discord_username=data.get("discord_username", "")
        )
    
    def is_serial_number(self) -> bool:
        """Check if adspower_id is a serial number (numeric)."""
        return self.adspower_id.isdigit() and len(self.adspower_id) > 0
    
    def get_serial_number(self) -> Optional[int]:
        """Get serial number if adspower_id is numeric."""
        return int(self.adspower_id) if self.is_serial_number() else None
    
    def get_profile_id(self) -> Optional[str]:
        """Get profile ID if adspower_id is not numeric."""
        return None if self.is_serial_number() else self.adspower_id
    
    def get_display_identifier(self) -> str:
        """Get human-readable identifier for logging."""
        return f"#{self.adspower_id}" if self.is_serial_number() else self.adspower_id


@dataclass
class ExecutionLogEntry:
    """Single execution log entry"""
    timestamp: str
    account: str
    action: str
    success: bool
    message: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# ACCOUNT MANAGER
# ============================================================================

class AccountManager:
    """Manages account configurations and execution status"""
    
    PLACEHOLDER_PATTERNS = [
        "YOUR_ADSPOWER_PROFILE_ID",
        "YOUR_PROFILE_ID",
        "PLACEHOLDER",
        "EXAMPLE_ID",
    ]
    
    def __init__(self, config_path: str = "config.json") -> None:
        """
        Initialize account manager.
        
        Args:
            config_path: Path to configuration JSON file
        """
        self.config_path = config_path
        self.config: Optional[Dict[str, Any]] = None
        self.accounts: List[Account] = []
        self.execution_log: List[ExecutionLogEntry] = []
        self._validation_errors: List[str] = []
        self._validation_warnings: List[str] = []
        
        # Файлы для разных типов блокировок
        self.blocked_accounts_file = "blocked_accounts.json"  # Без доступа к каналу
        self.unauthorized_accounts_file = "unauthorized_accounts.json"  # Не авторизован
        
        # Словари для хранения блокировок
        self._blocked_accounts: Dict[str, Dict[str, Any]] = {}  # Без доступа к каналу
        self._unauthorized_accounts: Dict[str, Dict[str, Any]] = {}  # Не авторизован
        
        # Настройки блокировок (загружаются из конфига)
        self.block_accounts_enabled = True  # По умолчанию включено
        
        self._load_blocked_accounts()
        self._load_unauthorized_accounts()
    
    # ========================================================================
    # CONFIG LOADING
    # ========================================================================
    
    def load_config(self) -> bool:
        """
        Load configuration from JSON file.
        Supports loading accounts from Google Sheets if configured.
        
        Returns:
            True if loaded successfully
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            
            # Загружаем настройки блокировок
            blocking_config = self.config.get("account_blocking", {})
            self.block_accounts_enabled = blocking_config.get("enabled", True)
            
            # Try Google Sheets first, then fall back to config file
            if not self._try_load_from_google_sheets():
                self._load_accounts_from_config()
            
            return self._validate_config()
            
        except FileNotFoundError:
            print(f"❌ Config file not found: {self.config_path}")
            return False
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in config file: {e}")
            return False
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            return False
    
    def _load_accounts_from_config(self) -> None:
        """Load accounts from config file."""
        raw_accounts = self.config.get("accounts", [])
        self.accounts = [
            Account.from_dict(acc) 
            for acc in raw_accounts 
            if isinstance(acc, dict)
        ]
    
    def _try_load_from_google_sheets(self) -> bool:
        """
        Try to load accounts from Google Sheets.
        
        Returns:
            True if loaded from Google Sheets, False to use config file
        """
        gs_config = self.config.get("google_sheets", {})
        gs_url = gs_config.get("url", "") or self.config.get("google_sheets_url", "")
        
        if not gs_url or not gs_config.get("enabled", True):
            return False
        
        try:
            credentials_path = gs_config.get("credentials_path")
            mode = "Service Account" if credentials_path else "публичный доступ"
            print(f"📊 Загрузка аккаунтов из Google Sheets ({mode})...")
            
            reader = create_reader(
                gs_url,
                credentials_path=credentials_path,
                sheet_name=gs_config.get("sheet_name"),
                sheet_gid=gs_config.get("sheet_gid")
            )
            
            # Show service account email
            if credentials_path and hasattr(reader, 'get_service_account_email'):
                email = reader.get_service_account_email()
                if email:
                    print(f"   Service Account: {email}")
            
            raw_accounts, warnings = reader.fetch_accounts()
            
            for warning in warnings:
                print(f"⚠️ {warning}")
            
            self.accounts = [Account.from_dict(acc) for acc in raw_accounts]
            self.config["accounts"] = raw_accounts
            # Лог о загрузке уже выводится в fetch_accounts()
            return True
            
        except FileNotFoundError as e:
            print(f"❌ {e}")
        except PermissionError as e:
            print(f"❌ {e}")
        except ImportError as e:
            print(f"❌ {e}")
        except ValueError as e:
            print(f"❌ Ошибка Google Sheets: {e}")
        except ConnectionError as e:
            print(f"❌ Ошибка подключения к Google Sheets: {e}")
        except Exception as e:
            print(f"❌ Не удалось загрузить из Google Sheets: {e}")
        
        return False
    
    # ========================================================================
    # VALIDATION
    # ========================================================================
    
    def _validate_config(self) -> bool:
        """Validate configuration structure and values."""
        self._validation_errors.clear()
        self._validation_warnings.clear()
        
        if not self.config:
            self._validation_errors.append("Configuration is empty")
            self._print_validation_results()
            return False
        
        self._validate_discord_url()
        self._validate_delays()
        self._validate_accounts_exist()
        self._validate_account_fields()
        
        self._print_validation_results()
        return len(self._validation_errors) == 0
    
    def _validate_discord_url(self) -> None:
        """Validate Discord channel URL."""
        channel_url = self.config.get("discord_channel_url", "")
        
        if not channel_url:
            self._validation_errors.append("discord_channel_url is not configured")
        elif not channel_url.startswith("https://discord.com/"):
            self._validation_errors.append(
                f"Invalid discord_channel_url: {channel_url}\n"
                "   Must start with: https://discord.com/"
            )
    
    def _validate_delays(self) -> None:
        """Validate delay values."""
        for key in ["delay_between_accounts", "delay_between_commands"]:
            value = self.config.get(key, 0)
            if not isinstance(value, (int, float)) or value < 0:
                self._validation_errors.append(
                    f"Invalid {key}: {value} (must be non-negative number)"
                )
    
    def _validate_accounts_exist(self) -> None:
        """Validate that accounts are configured."""
        if not self.accounts:
            gs_url = (
                self.config.get("google_sheets", {}).get("url", "") or 
                self.config.get("google_sheets_url", "")
            )
            if gs_url:
                self._validation_errors.append(
                    "Не удалось загрузить аккаунты из Google Sheets"
                )
            else:
                self._validation_errors.append(
                    "No accounts configured (add to config or use google_sheets_url)"
                )
    
    def _validate_account_fields(self) -> None:
        """Validate each account's fields."""
        for i, account in enumerate(self.accounts):
            num = i + 1
            
            if not account.name:
                self._validation_errors.append(f"Account {num}: 'name' is required")
            
            self._validate_adspower_id(account, num)
            self._validate_discord_username(account, num)
    
    def _validate_adspower_id(self, account: Account, num: int) -> None:
        """Validate account's adspower_id."""
        if not account.adspower_id:
            self._validation_errors.append(
                f"Account {num} ({account.name}): 'adspower_id' is required"
            )
        elif self._is_placeholder_value(account.adspower_id):
            self._validation_errors.append(
                f"Account {num} ({account.name}): adspower_id contains placeholder value\n"
                f"   Please replace '{account.adspower_id}' with actual AdsPower profile ID"
            )
        elif account.is_serial_number():
            serial = account.get_serial_number()
            if serial is not None and serial <= 0:
                self._validation_errors.append(
                    f"Account {num} ({account.name}): serial number must be positive"
                )
    
    def _validate_discord_username(self, account: Account, num: int) -> None:
        """Validate account's discord_username."""
        if not account.discord_username:
            self._validation_errors.append(
                f"Account {num} ({account.name}): 'discord_username' is required"
            )
        elif account.discord_username.lower().startswith("username"):
            self._validation_warnings.append(
                f"Account {num} ({account.name}): discord_username looks like placeholder"
            )
    
    def _is_placeholder_value(self, value: str) -> bool:
        """Check if value looks like a placeholder."""
        value_upper = value.upper()
        return any(pattern in value_upper for pattern in self.PLACEHOLDER_PATTERNS)
    
    def _print_validation_results(self) -> None:
        """Print validation errors and warnings."""
        for error in self._validation_errors:
            print(f"❌ {error}")
        
        for warning in self._validation_warnings:
            print(f"⚠️ {warning}")
        
        if not self._validation_errors and not self._validation_warnings:
            print("✅ Configuration validated successfully")
        elif not self._validation_errors:
            print(f"✅ Configuration valid with {len(self._validation_warnings)} warning(s)")
    
    # ========================================================================
    # ACCOUNT QUERIES
    # ========================================================================
    
    def get_account_pairs(self) -> List[Dict[str, Any]]:
        """
        Get account pairs for blessing/cursing (паровозик).
        Each account blesses/curses the next one in the chain.
        """
        if not self.accounts:
            print("⚠️ No accounts configured")
            return []
        
        if len(self.accounts) < 2:
            print("⚠️ Need at least 2 accounts for chain mode")
        
        pairs = []
        total = len(self.accounts)
        
        for i, account in enumerate(self.accounts):
            next_index = (i + 1) % total
            target_account = self.accounts[next_index]
            
            pairs.append({
                "current": account.to_dict(),
                "target": target_account.to_dict(),
                "index": i + 1,
                "total": total
            })
        
        return pairs
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config.get(key, default) if self.config else default
    
    # ========================================================================
    # EXECUTION LOGGING
    # ========================================================================
    
    def log_execution(
        self, 
        account_name: str, 
        action: str, 
        success: bool, 
        message: str = ""
    ) -> None:
        """Log execution result for an account action."""
        self.execution_log.append(ExecutionLogEntry(
            timestamp=datetime.now().isoformat(),
            account=account_name,
            action=action,
            success=success,
            message=message
        ))
    
    def get_execution_stats(self) -> Dict[str, Any]:
        """Get execution statistics."""
        command_actions = ["bless", "curse"]
        command_logs = [
            log for log in self.execution_log 
            if log.action in command_actions
        ]
        
        total = len(command_logs)
        successful = len([log for log in command_logs if log.success])
        
        return {
            "total": total,
            "successful": successful,
            "failed": total - successful,
            "success_rate": (successful / total * 100) if total > 0 else 0.0
        }
    
    def print_summary(self) -> None:
        """Print execution summary to console."""
        print("\n" + "="*60)
        print("📊 EXECUTION SUMMARY")
        print("="*60)
        
        stats = self.get_execution_stats()
        
        print(f"Total actions: {stats['total']}")
        print(f"✅ Successful: {stats['successful']}")
        print(f"❌ Failed: {stats['failed']}")
        
        if stats['total'] > 0:
            print(f"🎯 Success rate: {stats['success_rate']:.1f}%")
        
        # List failed actions
        command_actions = ["bless", "curse"]
        failed_logs = [
            log for log in self.execution_log 
            if not log.success and log.action in command_actions
        ]
        
        if failed_logs:
            print("\n⚠️ Failed actions:")
            for log in failed_logs:
                print(f"  - {log.account}: {log.action} - {log.message}")
        
        print("="*60 + "\n")
    
    def save_log(self, filename: str = "execution_log.json") -> None:
        """Save execution log to JSON file."""
        try:
            log_data = [entry.to_dict() for entry in self.execution_log]
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=2, ensure_ascii=False)
            print(f"✅ Execution log saved to: {filename}")
        except Exception as e:
            print(f"❌ Error saving log: {e}")
    
    # ========================================================================
    # BLOCKED ACCOUNTS MANAGEMENT
    # ========================================================================
    
    def _load_blocked_accounts(self) -> None:
        """Загрузить список заблокированных аккаунтов (без доступа к каналу) из файла."""
        try:
            if os.path.exists(self.blocked_accounts_file):
                with open(self.blocked_accounts_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self._blocked_accounts = data.get("blocked_accounts", {})
                    logger.info(f"Loaded {len(self._blocked_accounts)} blocked accounts (no channel access)")
            else:
                self._blocked_accounts = {}
        except Exception as e:
            logger.error(f"Error loading blocked accounts: {e}")
            self._blocked_accounts = {}
    
    def _load_unauthorized_accounts(self) -> None:
        """Загрузить список неавторизованных аккаунтов из файла."""
        try:
            if os.path.exists(self.unauthorized_accounts_file):
                with open(self.unauthorized_accounts_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self._unauthorized_accounts = data.get("unauthorized_accounts", {})
                    logger.info(f"Loaded {len(self._unauthorized_accounts)} unauthorized accounts")
            else:
                self._unauthorized_accounts = {}
        except Exception as e:
            logger.error(f"Error loading unauthorized accounts: {e}")
            self._unauthorized_accounts = {}
    
    def _save_blocked_accounts(self) -> None:
        """Сохранить список заблокированных аккаунтов (без доступа к каналу) в файл."""
        try:
            data = {
                "blocked_accounts": self._blocked_accounts,
                "last_updated": datetime.now().isoformat()
            }
            with open(self.blocked_accounts_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(self._blocked_accounts)} blocked accounts (no channel access)")
        except Exception as e:
            logger.error(f"Error saving blocked accounts: {e}")
    
    def _save_unauthorized_accounts(self) -> None:
        """Сохранить список неавторизованных аккаунтов в файл."""
        try:
            data = {
                "unauthorized_accounts": self._unauthorized_accounts,
                "last_updated": datetime.now().isoformat()
            }
            with open(self.unauthorized_accounts_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(self._unauthorized_accounts)} unauthorized accounts")
        except Exception as e:
            logger.error(f"Error saving unauthorized accounts: {e}")
    
    def is_account_blocked(self, account_name: str, adspower_id: Optional[str] = None) -> bool:
        """
        Проверить, заблокирован ли аккаунт (любой тип блокировки).
        
        Args:
            account_name: Имя аккаунта
            adspower_id: Опциональный AdsPower ID для дополнительной проверки
            
        Returns:
            True если аккаунт заблокирован (неавторизован или без доступа к каналу)
        """
        # Проверяем оба типа блокировок
        return (self.is_account_unauthorized(account_name, adspower_id) or 
                self.is_account_channel_blocked(account_name, adspower_id))
    
    def is_account_unauthorized(self, account_name: str, adspower_id: Optional[str] = None) -> bool:
        """
        Проверить, неавторизован ли аккаунт.
        
        Args:
            account_name: Имя аккаунта
            adspower_id: Опциональный AdsPower ID для дополнительной проверки
            
        Returns:
            True если аккаунт неавторизован
        """
        if account_name in self._unauthorized_accounts:
            return True
        
        if adspower_id:
            for data in self._unauthorized_accounts.values():
                if data.get("adspower_id") == adspower_id:
                    return True
        
        return False
    
    def is_account_channel_blocked(self, account_name: str, adspower_id: Optional[str] = None) -> bool:
        """
        Проверить, заблокирован ли аккаунт из-за отсутствия доступа к каналу.
        
        Args:
            account_name: Имя аккаунта
            adspower_id: Опциональный AdsPower ID для дополнительной проверки
            
        Returns:
            True если аккаунт заблокирован из-за отсутствия доступа к каналу
        """
        if account_name in self._blocked_accounts:
            return True
        
        if adspower_id:
            for data in self._blocked_accounts.values():
                if data.get("adspower_id") == adspower_id:
                    return True
        
        return False
    
    def block_account(
        self, 
        account_name: str, 
        adspower_id: str, 
        reason: str = "Нет доступа к каналу",
        discord_username: Optional[str] = None,
        block_type: str = "channel"
    ) -> None:
        """
        Заблокировать аккаунт (добавить в соответствующий список).
        
        Args:
            account_name: Имя аккаунта
            adspower_id: AdsPower ID аккаунта
            reason: Причина блокировки
            discord_username: Discord username (опционально)
            block_type: Тип блокировки - "channel" (без доступа к каналу) или "unauthorized" (не авторизован)
        """
        if not self.block_accounts_enabled:
            logger.info(f"Account blocking is disabled, skipping block for {account_name}")
            return
        
        if block_type == "unauthorized":
            self._block_unauthorized_account(account_name, adspower_id, reason, discord_username)
        else:
            self._block_channel_account(account_name, adspower_id, reason, discord_username)
    
    def _block_unauthorized_account(
        self,
        account_name: str,
        adspower_id: str,
        reason: str,
        discord_username: Optional[str] = None
    ) -> None:
        """Заблокировать неавторизованный аккаунт."""
        if self.is_account_unauthorized(account_name, adspower_id):
            logger.info(f"Account {account_name} already marked as unauthorized")
            return
        
        self._unauthorized_accounts[account_name] = {
            "account_name": account_name,
            "adspower_id": adspower_id,
            "discord_username": discord_username,
            "reason": reason,
            "blocked_at": datetime.now().isoformat()
        }
        
        self._save_unauthorized_accounts()
        logger.warning(f"Account {account_name} marked as unauthorized: {reason}")
        print(f"🚫 Аккаунт {account_name} помечен как неавторизованный: {reason}")
    
    def _block_channel_account(
        self,
        account_name: str,
        adspower_id: str,
        reason: str,
        discord_username: Optional[str] = None
    ) -> None:
        """Заблокировать аккаунт из-за отсутствия доступа к каналу."""
        if self.is_account_channel_blocked(account_name, adspower_id):
            logger.info(f"Account {account_name} already blocked (no channel access)")
            return
        
        self._blocked_accounts[account_name] = {
            "account_name": account_name,
            "adspower_id": adspower_id,
            "discord_username": discord_username,
            "reason": reason,
            "blocked_at": datetime.now().isoformat()
        }
        
        self._save_blocked_accounts()
        logger.warning(f"Account {account_name} blocked (no channel access): {reason}")
        print(f"🚫 Аккаунт {account_name} заблокирован (нет доступа к каналу): {reason}")
    
    def unblock_account(self, account_name: str, block_type: Optional[str] = None) -> bool:
        """
        Разблокировать аккаунт (удалить из списка заблокированных).
        
        Args:
            account_name: Имя аккаунта
            block_type: Тип блокировки для разблокировки - "channel", "unauthorized" или None (оба)
            
        Returns:
            True если аккаунт был разблокирован
        """
        unblocked = False
        
        if block_type is None or block_type == "channel":
            if account_name in self._blocked_accounts:
                del self._blocked_accounts[account_name]
                self._save_blocked_accounts()
                logger.info(f"Account {account_name} unblocked (channel access)")
                unblocked = True
        
        if block_type is None or block_type == "unauthorized":
            if account_name in self._unauthorized_accounts:
                del self._unauthorized_accounts[account_name]
                self._save_unauthorized_accounts()
                logger.info(f"Account {account_name} unblocked (unauthorized)")
                unblocked = True
        
        return unblocked
    
    def filter_blocked_accounts(self, accounts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Отфильтровать заблокированные аккаунты из списка.
        
        Args:
            accounts: Список аккаунтов для фильтрации
            
        Returns:
            Отфильтрованный список без заблокированных аккаунтов
        """
        filtered = []
        blocked_count = 0
        
        for acc in accounts:
            account_name = acc.get("name", "")
            adspower_id = acc.get("adspower_id", "")
            
            if self.is_account_blocked(account_name, adspower_id):
                blocked_count += 1
                blocked_data = self._blocked_accounts.get(account_name, {})
                reason = blocked_data.get("reason", "Неизвестная причина")
                logger.debug(f"Skipping blocked account: {account_name} ({reason})")
            else:
                filtered.append(acc)
        
        if blocked_count > 0:
            print(f"⚠️ Пропущено {blocked_count} заблокированных аккаунтов")
        
        return filtered
    
    def get_blocked_accounts_list(self, block_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получить список заблокированных аккаунтов.
        
        Args:
            block_type: Тип блокировки - "channel", "unauthorized" или None (оба)
            
        Returns:
            Список заблокированных аккаунтов
        """
        if block_type == "channel":
            return list(self._blocked_accounts.values())
        elif block_type == "unauthorized":
            return list(self._unauthorized_accounts.values())
        else:
            # Возвращаем оба списка с пометкой типа
            result = []
            for data in self._blocked_accounts.values():
                result.append({**data, "block_type": "channel"})
            for data in self._unauthorized_accounts.values():
                result.append({**data, "block_type": "unauthorized"})
            return result
    
    def print_blocked_accounts(self) -> None:
        """Вывести список всех заблокированных аккаунтов."""
        has_blocked = len(self._blocked_accounts) > 0
        has_unauthorized = len(self._unauthorized_accounts) > 0
        
        if not has_blocked and not has_unauthorized:
            print("✅ Нет заблокированных аккаунтов")
            return
        
        print("\n" + "="*60)
        print("🚫 ЗАБЛОКИРОВАННЫЕ АККАУНТЫ")
        print("="*60)
        
        if has_unauthorized:
            print("\n📋 Неавторизованные аккаунты:")
            for account_name, data in self._unauthorized_accounts.items():
                reason = data.get("reason", "Неизвестная причина")
                blocked_at = data.get("blocked_at", "")
                adspower_id = data.get("adspower_id", "")
                
                print(f"\n  🚫 {account_name}")
                print(f"     AdsPower ID: {adspower_id}")
                print(f"     Причина: {reason}")
                if blocked_at:
                    print(f"     Заблокирован: {blocked_at}")
        
        if has_blocked:
            print("\n📋 Аккаунты без доступа к каналу:")
            for account_name, data in self._blocked_accounts.items():
                reason = data.get("reason", "Неизвестная причина")
                blocked_at = data.get("blocked_at", "")
                adspower_id = data.get("adspower_id", "")
                
                print(f"\n  🚫 {account_name}")
                print(f"     AdsPower ID: {adspower_id}")
                print(f"     Причина: {reason}")
                if blocked_at:
                    print(f"     Заблокирован: {blocked_at}")
        
        print("="*60 + "\n")
