"""
Account Manager
Manages account configurations and execution tracking
"""
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, asdict


@dataclass
class Account:
    """Represents a Discord account with AdsPower profile"""
    name: str
    adspower_id: str  # Can be profile ID (string) or serial number (numeric string)
    discord_username: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Account':
        """Create Account from dictionary"""
        adspower_id = data.get("adspower_id", "")
        # Convert to string if it's a number
        if isinstance(adspower_id, (int, float)):
            adspower_id = str(int(adspower_id))
        
        return cls(
            name=data.get("name", ""),
            adspower_id=str(adspower_id) if adspower_id else "",
            discord_username=data.get("discord_username", "")
        )
    
    def is_serial_number(self) -> bool:
        """
        Check if adspower_id is a serial number (numeric) or profile ID (alphanumeric)
        
        Returns:
            bool: True if adspower_id is a numeric serial number
        """
        return self.adspower_id.isdigit() and len(self.adspower_id) > 0
    
    def get_serial_number(self) -> Optional[int]:
        """
        Get serial number if adspower_id is numeric
        
        Returns:
            int or None: Serial number if numeric, None otherwise
        """
        if self.is_serial_number():
            return int(self.adspower_id)
        return None
    
    def get_profile_id(self) -> Optional[str]:
        """
        Get profile ID if adspower_id is not numeric
        
        Returns:
            str or None: Profile ID if alphanumeric, None otherwise
        """
        if not self.is_serial_number():
            return self.adspower_id
        return None
    
    def get_display_identifier(self) -> str:
        """Get human-readable identifier for logging"""
        if self.is_serial_number():
            return f"#{self.adspower_id}"
        return self.adspower_id


@dataclass
class ExecutionLogEntry:
    """Single execution log entry"""
    timestamp: str
    account: str
    action: str
    success: bool
    message: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


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
        Initialize account manager
        
        Args:
            config_path: Path to configuration JSON file
        """
        self.config_path = config_path
        self.config: Optional[Dict[str, Any]] = None
        self.accounts: List[Account] = []
        self.execution_log: List[ExecutionLogEntry] = []
        self._validation_errors: List[str] = []
        self._validation_warnings: List[str] = []
        
    def load_config(self) -> bool:
        """
        Load configuration from JSON file
        
        Returns:
            bool: True if loaded successfully
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            
            # Parse accounts into Account objects
            raw_accounts = self.config.get("accounts", [])
            self.accounts = [Account.from_dict(acc) for acc in raw_accounts if isinstance(acc, dict)]
            
            # Validate configuration
            if not self._validate_config():
                return False
            
            print(f"✅ Loaded {len(self.accounts)} accounts from config")
            return True
            
        except FileNotFoundError:
            print(f"❌ Config file not found: {self.config_path}")
            return False
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in config file: {e}")
            return False
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            return False
    
    def _is_placeholder_value(self, value: str) -> bool:
        """Check if value looks like a placeholder"""
        value_upper = value.upper()
        return any(pattern in value_upper for pattern in self.PLACEHOLDER_PATTERNS)
    
    def _validate_config(self) -> bool:
        """
        Validate configuration structure and values
        
        Returns:
            bool: True if configuration is valid
            
        Raises:
            ConfigurationError: If configuration has critical errors
        """
        self._validation_errors = []
        self._validation_warnings = []
        
        # Check required fields
        if not self.config:
            self._validation_errors.append("Configuration is empty")
            self._print_validation_results()
            return False
        
        # Validate Discord channel URL
        channel_url = self.config.get("discord_channel_url", "")
        if not channel_url:
            self._validation_errors.append("discord_channel_url is not configured")
        elif not channel_url.startswith("https://discord.com/"):
            self._validation_errors.append(
                f"Invalid discord_channel_url: {channel_url}\n"
                "   Must start with: https://discord.com/"
            )
        
        # Validate delay values
        delay_between_accounts = self.config.get("delay_between_accounts", 8)
        delay_between_commands = self.config.get("delay_between_commands", 5)
        
        if not isinstance(delay_between_accounts, (int, float)) or delay_between_accounts < 0:
            self._validation_errors.append(
                f"Invalid delay_between_accounts: {delay_between_accounts} (must be non-negative number)"
            )
        
        if not isinstance(delay_between_commands, (int, float)) or delay_between_commands < 0:
            self._validation_errors.append(
                f"Invalid delay_between_commands: {delay_between_commands} (must be non-negative number)"
            )
        
        # Validate accounts
        if not self.accounts:
            self._validation_errors.append("No accounts configured")
        
        # Validate each account
        for i, account in enumerate(self.accounts):
            account_num = i + 1
            
            if not account.name:
                self._validation_errors.append(f"Account {account_num}: 'name' is required")
            
            # Validate AdsPower profile identifier
            if not account.adspower_id:
                self._validation_errors.append(
                    f"Account {account_num} ({account.name}): 'adspower_id' is required"
                )
            elif self._is_placeholder_value(account.adspower_id):
                # Placeholder in adspower_id is an ERROR
                self._validation_errors.append(
                    f"Account {account_num} ({account.name}): adspower_id contains placeholder value\n"
                    f"   Please replace '{account.adspower_id}' with actual AdsPower profile ID or serial number"
                )
            elif account.is_serial_number():
                # Validate serial number is positive
                serial = account.get_serial_number()
                if serial is not None and serial <= 0:
                    self._validation_errors.append(
                        f"Account {account_num} ({account.name}): serial number must be a positive integer"
                    )
            
            if not account.discord_username:
                self._validation_errors.append(
                    f"Account {account_num} ({account.name}): 'discord_username' is required"
                )
            elif account.discord_username.lower().startswith("username"):
                # Username placeholder is a warning (might be intentional)
                self._validation_warnings.append(
                    f"Account {account_num} ({account.name}): discord_username looks like a placeholder: {account.discord_username}"
                )
        
        # Print results and return
        self._print_validation_results()
        return len(self._validation_errors) == 0
    
    def _print_validation_results(self) -> None:
        """Print validation errors and warnings"""
        for error in self._validation_errors:
            print(f"❌ {error}")
        
        for warning in self._validation_warnings:
            print(f"⚠️ {warning}")
        
        if not self._validation_errors and not self._validation_warnings:
            print("✅ Configuration validated successfully")
        elif not self._validation_errors:
            print(f"✅ Configuration valid with {len(self._validation_warnings)} warning(s)")
    
    def get_account_pairs(self) -> List[Dict[str, Any]]:
        """
        Get account pairs for blessing/cursing (паровозик)
        Each account blesses/curses the next one in the chain
        
        Returns:
            list: List of dicts with 'current' and 'target' account info
        """
        if not self.accounts:
            print("⚠️ No accounts configured")
            return []
        
        if len(self.accounts) < 2:
            print("⚠️ Need at least 2 accounts for chain mode")
            # Single account can still target itself (if allowed by Discord)
        
        pairs = []
        total = len(self.accounts)
        
        for i, account in enumerate(self.accounts):
            # Next account in chain (wraps around to first)
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
        """
        Get configuration value
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        return self.config.get(key, default) if self.config else default
    
    def log_execution(self, account_name: str, action: str, success: bool, message: str = "") -> None:
        """
        Log execution result for an account action
        
        Args:
            account_name: Name of the account
            action: Action performed (e.g., 'bless', 'curse', 'skip', 'error')
            success: Whether action was successful
            message: Additional context or error message
        """
        log_entry = ExecutionLogEntry(
            timestamp=datetime.now().isoformat(),
            account=account_name,
            action=action,
            success=success,
            message=message
        )
        self.execution_log.append(log_entry)
    
    def get_execution_stats(self) -> Dict[str, Any]:
        """
        Get execution statistics
        
        Returns:
            dict: Statistics including total, successful, failed counts
        """
        command_actions = ["bless", "curse"]
        command_logs = [log for log in self.execution_log if log.action in command_actions]
        
        total = len(command_logs)
        successful = len([log for log in command_logs if log.success])
        failed = total - successful
        
        return {
            "total": total,
            "successful": successful,
            "failed": failed,
            "success_rate": (successful / total * 100) if total > 0 else 0.0
        }
    
    def print_summary(self) -> None:
        """
        Print execution summary to console
        
        Displays:
        - Total actions performed
        - Successful vs failed actions
        - Success rate percentage
        - List of failed actions with details
        """
        print("\n" + "="*60)
        print("📊 EXECUTION SUMMARY")
        print("="*60)
        
        stats = self.get_execution_stats()
        
        print(f"Total actions: {stats['total']}")
        print(f"✅ Successful: {stats['successful']}")
        print(f"❌ Failed: {stats['failed']}")
        
        # Success rate
        if stats['total'] > 0:
            print(f"🎯 Success rate: {stats['success_rate']:.1f}%")
        
        # List failed actions
        command_actions = ["bless", "curse"]
        failed_logs = [log for log in self.execution_log if not log.success and log.action in command_actions]
        
        if failed_logs:
            print("\n⚠️ Failed actions:")
            for log in failed_logs:
                print(f"  - {log.account}: {log.action} - {log.message}")
        
        print("="*60 + "\n")
    
    def save_log(self, filename: str = "execution_log.json") -> None:
        """
        Save execution log to JSON file
        
        Args:
            filename: Output filename (default: execution_log.json)
        """
        try:
            log_data = [entry.to_dict() for entry in self.execution_log]
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=2, ensure_ascii=False)
            print(f"✅ Execution log saved to: {filename}")
        except Exception as e:
            print(f"❌ Error saving log: {e}")
