"""
State Manager
Отслеживание прогресса, дневных лимитов и истории действий
"""
import json
import os
import random
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from .logger_config import get_logger

logger = get_logger("StateManager")


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class AccountProgress:
    """Прогресс аккаунта по получению bless/curse"""
    bless_received: int = 0
    curse_received: int = 0
    bless_given_today: int = 0
    curse_given_today: int = 0
    last_action_date: str = ""
    last_action_time: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccountProgress':
        return cls(
            bless_received=data.get("bless_received", 0),
            curse_received=data.get("curse_received", 0),
            bless_given_today=data.get("bless_given_today", 0),
            curse_given_today=data.get("curse_given_today", 0),
            last_action_date=data.get("last_action_date", ""),
            last_action_time=data.get("last_action_time", "")
        )
    
    @property
    def total_given_today(self) -> int:
        """Total actions given today."""
        return self.bless_given_today + self.curse_given_today
    
    def reset_daily(self) -> None:
        """Reset daily counters."""
        self.bless_given_today = 0
        self.curse_given_today = 0


@dataclass
class DailyStats:
    """Статистика за день"""
    date: str
    accounts_processed: List[str] = field(default_factory=list)
    total_bless: int = 0
    total_curse: int = 0
    actions: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DailyStats':
        return cls(
            date=data.get("date", ""),
            accounts_processed=data.get("accounts_processed", []),
            total_bless=data.get("total_bless", 0),
            total_curse=data.get("total_curse", 0),
            actions=data.get("actions", [])
        )


# ============================================================================
# STATE MANAGER
# ============================================================================

class StateManager:
    """
    Управление состоянием автоматизации.
    
    Отслеживает:
    - Прогресс каждого аккаунта (сколько bless/curse получено)
    - Дневные лимиты (сколько выдано сегодня)
    - История всех действий
    """
    
    DEFAULT_DAILY_LIMIT = 5
    DEFAULT_TARGET_COUNT = 10
    
    def __init__(self, state_file: str = "state.json"):
        self.state_file = state_file
        self.accounts: Dict[str, AccountProgress] = {}
        self.daily_stats: Dict[str, DailyStats] = {}
        self.settings: Dict[str, Any] = self._default_settings()
        self._dirty = False  # Track if state needs saving
        
        self._load_state()
    
    def _default_settings(self) -> Dict[str, Any]:
        """Get default settings."""
        return {
            "daily_limit_per_account": self.DEFAULT_DAILY_LIMIT,
            "target_bless": self.DEFAULT_TARGET_COUNT,
            "target_curse": self.DEFAULT_TARGET_COUNT,
            "created_at": datetime.now().isoformat()
        }
    
    # ========================================================================
    # STATE PERSISTENCE
    # ========================================================================
    
    def _load_state(self) -> bool:
        """Загрузить состояние из файла."""
        if not os.path.exists(self.state_file):
            logger.info(f"State file not found, creating new: {self.state_file}")
            self._save_state()
            return True
        
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.settings = data.get("settings", self._default_settings())
            
            self.accounts = {
                name: AccountProgress.from_dict(progress)
                for name, progress in data.get("accounts", {}).items()
            }
            
            self.daily_stats = {
                day: DailyStats.from_dict(stats)
                for day, stats in data.get("daily_stats", {}).items()
            }
            
            self._reset_daily_counters_if_needed()
            
            logger.debug(f"State loaded: {len(self.accounts)} accounts, {len(self.daily_stats)} days")
            return True
            
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            return False
    
    def _save_state(self) -> bool:
        """Сохранить состояние в файл."""
        try:
            data = {
                "settings": self.settings,
                "accounts": {name: acc.to_dict() for name, acc in self.accounts.items()},
                "daily_stats": {day: stats.to_dict() for day, stats in self.daily_stats.items()},
                "last_updated": datetime.now().isoformat()
            }
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            self._dirty = False
            return True
        except Exception as e:
            logger.error(f"Error saving state: {e}")
            return False
    
    def save_if_dirty(self) -> None:
        """Save state only if it has been modified."""
        if self._dirty:
            self._save_state()
    
    # ========================================================================
    # HELPERS
    # ========================================================================
    
    @staticmethod
    def _get_today() -> str:
        """Получить сегодняшнюю дату в формате YYYY-MM-DD."""
        return date.today().isoformat()
    
    def _reset_daily_counters_if_needed(self) -> None:
        """Сбросить дневные счетчики если наступил новый день."""
        today = self._get_today()
        reset_count = 0
        
        for name, account in self.accounts.items():
            if account.last_action_date and account.last_action_date != today:
                account.reset_daily()
                reset_count += 1
        
        if reset_count > 0:
            logger.info(f"New day: reset daily counters for {reset_count} accounts")
            self._dirty = True
    
    def _ensure_account_exists(self, account_name: str) -> AccountProgress:
        """Создать запись для аккаунта если не существует."""
        if account_name not in self.accounts:
            self.accounts[account_name] = AccountProgress()
            logger.info(f"Created new account progress: {account_name}")
            self._dirty = True
        return self.accounts[account_name]
    
    def _get_or_create_daily_stats(self) -> DailyStats:
        """Получить или создать статистику за сегодня."""
        today = self._get_today()
        if today not in self.daily_stats:
            self.daily_stats[today] = DailyStats(date=today)
            self._dirty = True
        return self.daily_stats[today]
    
    # ========================================================================
    # ACCOUNT QUERIES
    # ========================================================================
    
    def get_account_progress(self, account_name: str) -> AccountProgress:
        """Получить прогресс аккаунта."""
        return self._ensure_account_exists(account_name)
    
    def can_give_action_today(self, account_name: str) -> Tuple[bool, str]:
        """
        Проверить может ли аккаунт выполнить действие сегодня.
        
        Returns:
            (can_do, reason): Можно ли выполнить и причина
        """
        self._reset_daily_counters_if_needed()
        
        account = self._ensure_account_exists(account_name)
        daily_limit = self.settings["daily_limit_per_account"]
        
        if account.total_given_today >= daily_limit:
            return False, f"Daily limit reached ({account.total_given_today}/{daily_limit})"
        
        remaining = daily_limit - account.total_given_today
        return True, f"Can do {remaining} more actions today"
    
    def needs_bless(self, account_name: str) -> Tuple[bool, int]:
        """Проверить нужны ли ещё bless аккаунту."""
        account = self._ensure_account_exists(account_name)
        target = self.settings["target_bless"]
        remaining = max(0, target - account.bless_received)
        return remaining > 0, remaining
    
    def needs_curse(self, account_name: str) -> Tuple[bool, int]:
        """Проверить нужны ли ещё curse аккаунту."""
        account = self._ensure_account_exists(account_name)
        target = self.settings["target_curse"]
        remaining = max(0, target - account.curse_received)
        return remaining > 0, remaining
    
    def get_remaining_today(self, account_name: str) -> int:
        """Get remaining actions for today."""
        account = self._ensure_account_exists(account_name)
        daily_limit = self.settings["daily_limit_per_account"]
        return max(0, daily_limit - account.total_given_today)
    
    # ========================================================================
    # ACTION RECORDING
    # ========================================================================
    
    def record_action(
        self, 
        giver_name: str, 
        receiver_name: str, 
        action_type: str, 
        success: bool
    ) -> None:
        """
        Записать выполненное действие.
        
        Args:
            giver_name: Кто выдаёт (активный аккаунт)
            receiver_name: Кто получает (цель)
            action_type: "bless" или "curse"
            success: Успешно ли выполнено
        """
        giver = self._ensure_account_exists(giver_name)
        receiver = self._ensure_account_exists(receiver_name)
        
        now = datetime.now()
        today = self._get_today()
        
        if success:
            if action_type == "bless":
                giver.bless_given_today += 1
                receiver.bless_received += 1
            elif action_type == "curse":
                giver.curse_given_today += 1
                receiver.curse_received += 1
        
        giver.last_action_date = today
        giver.last_action_time = now.strftime("%H:%M:%S")
        
        # Update daily stats
        daily = self._get_or_create_daily_stats()
        if giver_name not in daily.accounts_processed:
            daily.accounts_processed.append(giver_name)
        
        if success:
            if action_type == "bless":
                daily.total_bless += 1
            elif action_type == "curse":
                daily.total_curse += 1
        
        daily.actions.append({
            "time": now.strftime("%H:%M:%S"),
            "giver": giver_name,
            "receiver": receiver_name,
            "action": action_type,
            "success": success
        })
        
        self._save_state()
        
        logger.info(f"Recorded: {giver_name} -> {action_type} -> {receiver_name} (success={success})")
    
    # ========================================================================
    # PAIR GENERATION
    # ========================================================================
    
    def get_optimal_pairs(
        self, 
        accounts: List[Dict[str, Any]], 
        max_actions: int = 10,
        account_mgr: Optional[Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Получить оптимальный список пар на сегодня.
        
        Логика:
        1. Фильтруем аккаунты которые могут выдавать
        2. Находим аккаунты которым нужны bless/curse (исключая заблокированные)
        3. Строим пары с равномерным распределением
        4. Перемешиваем пары для случайного порядка выполнения
        
        Returns:
            Список пар действий в случайном порядке
        """
        # Initialize all accounts
        for acc in accounts:
            self._ensure_account_exists(acc["name"])
        
        self.save_if_dirty()
        
        # Find available givers (исключая заблокированные)
        available_givers = self._get_available_givers(accounts, account_mgr)
        if not available_givers:
            logger.warning("No accounts available to give actions today")
            return []
        
        # Find accounts needing bless/curse (исключая заблокированные)
        needs_list = self._get_accounts_needing_actions(accounts, account_mgr)
        if not needs_list:
            logger.info("All accounts have reached their targets!")
            return []
        
        # Build pairs with even distribution
        pairs = self._build_pairs_even(available_givers, needs_list, max_actions)
        
        # Перемешиваем пары для случайного порядка выполнения
        random.shuffle(pairs)
        
        # Обновляем индексы после перемешивания
        for i, pair in enumerate(pairs, 1):
            pair["index"] = i
        
        logger.info(f"Planned {len(pairs)} actions for today (randomized order)")
        return pairs
    
    def _get_available_givers(
        self, 
        accounts: List[Dict[str, Any]], 
        account_mgr: Optional[Any] = None
    ) -> List[Dict[str, Any]]:
        """Get list of accounts that can give actions today (excluding blocked)."""
        available = []
        daily_limit = self.settings["daily_limit_per_account"]
        
        for acc in accounts:
            account_name = acc.get("name", "")
            adspower_id = acc.get("adspower_id", "")
            
            # Проверяем, не заблокирован ли аккаунт
            if account_mgr and account_mgr.is_account_blocked(account_name, adspower_id):
                continue
            
            can_give, _ = self.can_give_action_today(account_name)
            if can_give:
                progress = self.accounts[account_name]
                available.append({
                    **acc,
                    "remaining_today": daily_limit - progress.total_given_today,
                    "progress": progress
                })
        
        return available
    
    def _get_accounts_needing_actions(
        self, 
        accounts: List[Dict[str, Any]], 
        account_mgr: Optional[Any] = None
    ) -> List[Dict[str, Any]]:
        """Get list of accounts that need bless/curse (excluding blocked)."""
        needs_list = []
        
        for acc in accounts:
            account_name = acc.get("name", "")
            adspower_id = acc.get("adspower_id", "")
            
            # Пропускаем заблокированные аккаунты
            if account_mgr and account_mgr.is_account_blocked(account_name, adspower_id):
                continue
            
            needs_bless, bless_remaining = self.needs_bless(account_name)
            needs_curse, curse_remaining = self.needs_curse(account_name)
            
            if needs_bless or needs_curse:
                needs_list.append({
                    **acc,
                    "needs_bless": needs_bless,
                    "bless_remaining": bless_remaining,
                    "needs_curse": needs_curse,
                    "curse_remaining": curse_remaining,
                    "total_needed": bless_remaining + curse_remaining
                })
        
        # Sort by priority (most needed first)
        needs_list.sort(key=lambda x: x["total_needed"], reverse=True)
        return needs_list
    
    def _build_pairs_even(
        self, 
        givers: List[Dict[str, Any]], 
        receivers: List[Dict[str, Any]], 
        max_actions: int
    ) -> List[Dict[str, Any]]:
        """
        Построить пары действий с равномерным распределением между givers.
        
        Алгоритм:
        1. Создаём список всех нужных действий (bless/curse для каждого receiver)
        2. Сортируем по приоритету (больше нужных = выше приоритет)
        3. Распределяем действия равномерно между доступными givers
        4. Используем round-robin с учётом количества использований для равномерности
        
        Args:
            givers: Список доступных аккаунтов-отправителей
            receivers: Список аккаунтов-получателей с их потребностями
            max_actions: Максимальное количество действий
            
        Returns:
            Список пар действий
        """
        if not givers or not receivers:
            return []
        
        # Создаём список всех нужных действий
        action_queue = []
        for receiver in receivers:
            if receiver.get("needs_bless", False):
                action_queue.append({
                    "receiver": receiver,
                    "action": "bless",
                    "priority": receiver.get("bless_remaining", 0)
                })
            if receiver.get("needs_curse", False):
                action_queue.append({
                    "receiver": receiver,
                    "action": "curse",
                    "priority": receiver.get("curse_remaining", 0)
                })
        
        # Сортируем по приоритету (больше нужных = выше приоритет)
        action_queue.sort(key=lambda x: x["priority"], reverse=True)
        
        # Ограничиваем количество действий
        action_queue = action_queue[:max_actions]
        
        # Распределяем действия равномерно между givers
        pairs = []
        giver_usage = {i: 0 for i in range(len(givers))}  # Счётчик использований каждого giver
        giver_idx = 0
        
        for action_item in action_queue:
            receiver = action_item["receiver"]
            action_type = action_item["action"]
            
            # Находим доступного giver (не receiver, с оставшимися действиями)
            giver = None
            attempts = 0
            
            while attempts < len(givers):
                candidate = givers[giver_idx]
                
                # Проверяем условия
                if (candidate["name"] != receiver["name"] and 
                    candidate["remaining_today"] > 0):
                    giver = candidate
                    break
                
                giver_idx = (giver_idx + 1) % len(givers)
                attempts += 1
            
            if not giver:
                # Нет доступных givers - пропускаем это действие
                continue
            
            # Создаём пару
            pairs.append({
                "giver": giver,
                "receiver": receiver,
                "action": action_type,
                "index": len(pairs) + 1
            })
            
            # Обновляем счётчики
            giver["remaining_today"] -= 1
            giver_usage[givers.index(giver)] += 1
            
            # Переходим к следующему giver для равномерного распределения
            # Используем round-robin, но учитываем количество оставшихся действий
            giver_idx = self._find_next_giver_idx(givers, giver_idx, giver_usage)
        
        # Добавляем общее количество
        for pair in pairs:
            pair["total"] = len(pairs)
        
        return pairs
    
    def _find_next_giver_idx(
        self, 
        givers: List[Dict[str, Any]], 
        current_idx: int,
        usage: Dict[int, int]
    ) -> int:
        """
        Найти индекс следующего giver для равномерного распределения.
        Выбирает giver с наименьшим количеством использований.
        """
        if not givers:
            return 0
        
        # Находим минимальное количество использований
        min_usage = min(usage.values()) if usage else 0
        
        # Ищем giver с минимальным использованием, начиная со следующего
        for i in range(len(givers)):
            idx = (current_idx + 1 + i) % len(givers)
            if usage.get(idx, 0) == min_usage and givers[idx]["remaining_today"] > 0:
                return idx
        
        # Если не нашли, просто переходим к следующему
        return (current_idx + 1) % len(givers)
    
    def _find_available_giver(
        self, 
        givers: List[Dict[str, Any]], 
        receiver_name: str, 
        start_idx: int
    ) -> Optional[Dict[str, Any]]:
        """Find an available giver that is not the receiver."""
        for i in range(len(givers)):
            idx = (start_idx + i) % len(givers)
            giver = givers[idx]
            
            if giver["name"] != receiver_name and giver["remaining_today"] > 0:
                return giver
        
        return None
    
    # ========================================================================
    # REPORTING
    # ========================================================================
    
    def print_progress_report(self) -> None:
        """Вывести отчёт о прогрессе всех аккаунтов."""
        target_bless = self.settings["target_bless"]
        target_curse = self.settings["target_curse"]
        daily_limit = self.settings["daily_limit_per_account"]
        
        print("\n" + "="*70)
        print("📊 ПРОГРЕСС АККАУНТОВ")
        print("="*70)
        print(f"🎯 Цель: {target_bless} bless + {target_curse} curse на каждом")
        print(f"📅 Дневной лимит: {daily_limit} действий с аккаунта")
        print("-"*70)
        
        if not self.accounts:
            print("  Нет данных об аккаунтах")
            print("="*70 + "\n")
            return
        
        for name, progress in sorted(self.accounts.items()):
            self._print_account_progress(name, progress, target_bless, target_curse, daily_limit)
        
        self._print_total_progress(target_bless, target_curse)
    
    def _print_account_progress(
        self, 
        name: str, 
        progress: AccountProgress,
        target_bless: int,
        target_curse: int,
        daily_limit: int
    ) -> None:
        """Print progress for single account."""
        bless_pct = (progress.bless_received / target_bless * 100) if target_bless > 0 else 100
        curse_pct = (progress.curse_received / target_curse * 100) if target_curse > 0 else 100
        
        bless_bar = self._progress_bar(progress.bless_received, target_bless)
        curse_bar = self._progress_bar(progress.curse_received, target_curse)
        
        daily_remaining = daily_limit - progress.total_given_today
        status = "✅" if (bless_pct >= 100 and curse_pct >= 100) else "🔄"
        
        print(f"\n{status} {name}:")
        print(f"   Bless: {bless_bar} {progress.bless_received}/{target_bless}")
        print(f"   Curse: {curse_bar} {progress.curse_received}/{target_curse}")
        print(f"   Сегодня выдано: {progress.total_given_today}/{daily_limit} (осталось: {daily_remaining})")
        
        if progress.last_action_time:
            print(f"   Последнее действие: {progress.last_action_date} {progress.last_action_time}")
    
    def _print_total_progress(self, target_bless: int, target_curse: int) -> None:
        """Print total progress summary."""
        total_bless = sum(acc.bless_received for acc in self.accounts.values())
        total_curse = sum(acc.curse_received for acc in self.accounts.values())
        total_target = len(self.accounts) * (target_bless + target_curse)
        total_done = total_bless + total_curse
        
        pct = (total_done / total_target * 100) if total_target > 0 else 0
        
        print("\n" + "="*70)
        print(f"📈 Общий прогресс: {total_done}/{total_target} ({pct:.1f}%)")
        print("="*70 + "\n")
    
    @staticmethod
    def _progress_bar(current: int, target: int, width: int = 20) -> str:
        """Создать текстовый прогресс-бар."""
        if target == 0:
            return "█" * width
        
        filled = int(width * min(current / target, 1.0))
        empty = width - filled
        return "█" * filled + "░" * empty
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    
    def get_summary(self) -> Dict[str, Any]:
        """Получить сводку состояния."""
        target_bless = self.settings["target_bless"]
        target_curse = self.settings["target_curse"]
        
        completed = sum(
            1 for p in self.accounts.values()
            if p.bless_received >= target_bless and p.curse_received >= target_curse
        )
        
        return {
            "total_accounts": len(self.accounts),
            "completed": completed,
            "in_progress": len(self.accounts) - completed,
            "target_bless": target_bless,
            "target_curse": target_curse,
            "daily_limit": self.settings["daily_limit_per_account"]
        }
    
    def update_settings(self, **kwargs) -> None:
        """Обновить настройки."""
        for key, value in kwargs.items():
            if key in self.settings:
                self.settings[key] = value
                logger.info(f"Setting updated: {key} = {value}")
        self._save_state()
