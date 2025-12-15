"""
State Manager
Отслеживание прогресса, дневных лимитов и истории действий
"""
import json
import os
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from logger_config import setup_logger

logger = setup_logger("StateManager", log_to_file=True)


@dataclass
class AccountProgress:
    """Прогресс аккаунта по получению bless/curse"""
    bless_received: int = 0      # Сколько благословений получено (цель: 10)
    curse_received: int = 0      # Сколько проклятий получено (цель: 10)
    bless_given_today: int = 0   # Сколько благословений выдано сегодня
    curse_given_today: int = 0   # Сколько проклятий выдано сегодня
    last_action_date: str = ""   # Дата последнего действия
    last_action_time: str = ""   # Время последнего действия
    
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


class StateManager:
    """
    Управление состоянием автоматизации
    
    Отслеживает:
    - Прогресс каждого аккаунта (сколько bless/curse получено)
    - Дневные лимиты (сколько выдано сегодня)
    - История всех действий
    """
    
    DEFAULT_DAILY_LIMIT = 5  # Максимум bless + curse выданных с одного аккаунта в день
    DEFAULT_TARGET_COUNT = 10  # Цель: получить 10 bless и 10 curse
    
    def __init__(self, state_file: str = "state.json"):
        self.state_file = state_file
        self.accounts: Dict[str, AccountProgress] = {}
        self.daily_stats: Dict[str, DailyStats] = {}
        self.settings: Dict[str, Any] = {
            "daily_limit_per_account": self.DEFAULT_DAILY_LIMIT,
            "target_bless": self.DEFAULT_TARGET_COUNT,
            "target_curse": self.DEFAULT_TARGET_COUNT,
            "created_at": datetime.now().isoformat()
        }
        
        self._load_state()
    
    def _load_state(self) -> bool:
        """Загрузить состояние из файла"""
        if not os.path.exists(self.state_file):
            logger.info(f"State file not found, creating new: {self.state_file}")
            self._save_state()
            return True
        
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Загрузка настроек
            self.settings = data.get("settings", self.settings)
            
            # Загрузка прогресса аккаунтов
            accounts_data = data.get("accounts", {})
            self.accounts = {
                name: AccountProgress.from_dict(progress)
                for name, progress in accounts_data.items()
            }
            
            # Загрузка дневной статистики
            daily_data = data.get("daily_stats", {})
            self.daily_stats = {
                day: DailyStats.from_dict(stats)
                for day, stats in daily_data.items()
            }
            
            # Сброс дневных счетчиков если новый день
            self._reset_daily_counters_if_needed()
            
            logger.info(f"State loaded: {len(self.accounts)} accounts, {len(self.daily_stats)} days of history")
            return True
            
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            return False
    
    def _save_state(self) -> bool:
        """Сохранить состояние в файл"""
        try:
            data = {
                "settings": self.settings,
                "accounts": {name: acc.to_dict() for name, acc in self.accounts.items()},
                "daily_stats": {day: stats.to_dict() for day, stats in self.daily_stats.items()},
                "last_updated": datetime.now().isoformat()
            }
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            return True
        except Exception as e:
            logger.error(f"Error saving state: {e}")
            return False
    
    def _get_today(self) -> str:
        """Получить сегодняшнюю дату в формате YYYY-MM-DD"""
        return date.today().isoformat()
    
    def _reset_daily_counters_if_needed(self) -> None:
        """Сбросить дневные счетчики если наступил новый день"""
        today = self._get_today()
        
        for name, account in self.accounts.items():
            if account.last_action_date and account.last_action_date != today:
                logger.info(f"New day detected for {name}, resetting daily counters")
                account.bless_given_today = 0
                account.curse_given_today = 0
        
        self._save_state()
    
    def _ensure_account_exists(self, account_name: str) -> None:
        """Создать запись для аккаунта если не существует"""
        if account_name not in self.accounts:
            self.accounts[account_name] = AccountProgress()
            logger.info(f"Created new account progress: {account_name}")
    
    def _get_or_create_daily_stats(self) -> DailyStats:
        """Получить или создать статистику за сегодня"""
        today = self._get_today()
        if today not in self.daily_stats:
            self.daily_stats[today] = DailyStats(date=today)
        return self.daily_stats[today]
    
    def get_account_progress(self, account_name: str) -> AccountProgress:
        """Получить прогресс аккаунта"""
        self._ensure_account_exists(account_name)
        return self.accounts[account_name]
    
    def can_give_action_today(self, account_name: str, action_type: str = "any") -> Tuple[bool, str]:
        """
        Проверить может ли аккаунт выполнить действие сегодня
        
        Args:
            account_name: Имя аккаунта который выдаёт
            action_type: "bless", "curse" или "any"
            
        Returns:
            (can_do, reason): Можно ли выполнить и причина если нет
        """
        self._ensure_account_exists(account_name)
        self._reset_daily_counters_if_needed()
        
        account = self.accounts[account_name]
        daily_limit = self.settings["daily_limit_per_account"]
        
        total_given_today = account.bless_given_today + account.curse_given_today
        
        if total_given_today >= daily_limit:
            return False, f"Daily limit reached ({total_given_today}/{daily_limit})"
        
        remaining = daily_limit - total_given_today
        return True, f"Can do {remaining} more actions today"
    
    def needs_bless(self, account_name: str) -> Tuple[bool, int]:
        """
        Проверить нужны ли ещё bless аккаунту
        
        Returns:
            (needs, remaining): Нужно ли и сколько ещё нужно
        """
        self._ensure_account_exists(account_name)
        account = self.accounts[account_name]
        target = self.settings["target_bless"]
        remaining = max(0, target - account.bless_received)
        return remaining > 0, remaining
    
    def needs_curse(self, account_name: str) -> Tuple[bool, int]:
        """
        Проверить нужны ли ещё curse аккаунту
        
        Returns:
            (needs, remaining): Нужно ли и сколько ещё нужно
        """
        self._ensure_account_exists(account_name)
        account = self.accounts[account_name]
        target = self.settings["target_curse"]
        remaining = max(0, target - account.curse_received)
        return remaining > 0, remaining
    
    def record_action(self, giver_name: str, receiver_name: str, 
                      action_type: str, success: bool) -> None:
        """
        Записать выполненное действие
        
        Args:
            giver_name: Кто выдаёт (активный аккаунт)
            receiver_name: Кто получает (цель)
            action_type: "bless" или "curse"
            success: Успешно ли выполнено
        """
        self._ensure_account_exists(giver_name)
        self._ensure_account_exists(receiver_name)
        
        now = datetime.now()
        today = self._get_today()
        
        giver = self.accounts[giver_name]
        receiver = self.accounts[receiver_name]
        
        if success:
            # Обновляем счетчики выдачи
            if action_type == "bless":
                giver.bless_given_today += 1
                receiver.bless_received += 1
            elif action_type == "curse":
                giver.curse_given_today += 1
                receiver.curse_received += 1
        
        # Обновляем время последнего действия
        giver.last_action_date = today
        giver.last_action_time = now.strftime("%H:%M:%S")
        
        # Записываем в дневную статистику
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
        
        logger.info(
            f"Recorded: {giver_name} -> {action_type} -> {receiver_name} "
            f"(success={success})"
        )
    
    def get_optimal_pairs(self, accounts: List[Dict[str, Any]], 
                          max_actions: int = 10) -> List[Dict[str, Any]]:
        """
        Получить оптимальный список пар (кто кому выдаёт) на сегодня
        
        Логика:
        1. Фильтруем аккаунты которые могут выдавать (не достигли дневного лимита)
        2. Находим аккаунты которым нужны bless/curse
        3. Строим пары с приоритетом тем кому больше нужно
        
        Args:
            accounts: Список аккаунтов из конфига
            max_actions: Максимум действий за сессию
            
        Returns:
            Список пар для выполнения
        """
        pairs = []
        
        # Создаём записи для всех аккаунтов
        for acc in accounts:
            self._ensure_account_exists(acc["name"])
        
        # Находим кто может выдавать сегодня
        available_givers = []
        for acc in accounts:
            can_give, reason = self.can_give_action_today(acc["name"])
            if can_give:
                progress = self.get_account_progress(acc["name"])
                remaining = self.settings["daily_limit_per_account"] - (
                    progress.bless_given_today + progress.curse_given_today
                )
                available_givers.append({
                    **acc,
                    "remaining_today": remaining,
                    "progress": progress
                })
        
        if not available_givers:
            logger.warning("No accounts available to give actions today")
            return []
        
        # Находим кому нужны bless/curse
        needs_list = []
        for acc in accounts:
            needs_bless, bless_remaining = self.needs_bless(acc["name"])
            needs_curse, curse_remaining = self.needs_curse(acc["name"])
            
            if needs_bless or needs_curse:
                needs_list.append({
                    **acc,
                    "needs_bless": needs_bless,
                    "bless_remaining": bless_remaining,
                    "needs_curse": needs_curse,
                    "curse_remaining": curse_remaining,
                    "total_needed": bless_remaining + curse_remaining
                })
        
        if not needs_list:
            logger.info("All accounts have reached their targets!")
            return []
        
        # Сортируем по приоритету (кому больше нужно)
        needs_list.sort(key=lambda x: x["total_needed"], reverse=True)
        
        actions_planned = 0
        giver_idx = 0
        
        # Строим пары
        for receiver in needs_list:
            if actions_planned >= max_actions:
                break
            
            # Для каждого получателя определяем что ему нужно
            actions_for_receiver = []
            
            if receiver["needs_bless"]:
                actions_for_receiver.append("bless")
            if receiver["needs_curse"]:
                actions_for_receiver.append("curse")
            
            # Назначаем выдающего
            for action_type in actions_for_receiver:
                if actions_planned >= max_actions:
                    break
                
                # Ищем свободного выдающего (не себе)
                found_giver = None
                for i in range(len(available_givers)):
                    idx = (giver_idx + i) % len(available_givers)
                    giver = available_givers[idx]
                    
                    if giver["name"] == receiver["name"]:
                        continue  # Нельзя себе
                    
                    if giver["remaining_today"] > 0:
                        found_giver = giver
                        giver_idx = (idx + 1) % len(available_givers)
                        break
                
                if found_giver:
                    pairs.append({
                        "giver": found_giver,
                        "receiver": receiver,
                        "action": action_type,
                        "index": len(pairs) + 1
                    })
                    found_giver["remaining_today"] -= 1
                    actions_planned += 1
        
        # Добавляем общее количество
        for i, pair in enumerate(pairs):
            pair["total"] = len(pairs)
        
        logger.info(f"Planned {len(pairs)} actions for today")
        return pairs
    
    def print_progress_report(self) -> None:
        """Вывести отчёт о прогрессе всех аккаунтов"""
        print("\n" + "="*70)
        print("📊 ПРОГРЕСС АККАУНТОВ")
        print("="*70)
        
        target_bless = self.settings["target_bless"]
        target_curse = self.settings["target_curse"]
        daily_limit = self.settings["daily_limit_per_account"]
        
        print(f"🎯 Цель: {target_bless} bless + {target_curse} curse на каждом")
        print(f"📅 Дневной лимит: {daily_limit} действий с аккаунта")
        print("-"*70)
        
        if not self.accounts:
            print("  Нет данных об аккаунтах")
            print("="*70 + "\n")
            return
        
        for name, progress in sorted(self.accounts.items()):
            bless_pct = (progress.bless_received / target_bless * 100) if target_bless > 0 else 100
            curse_pct = (progress.curse_received / target_curse * 100) if target_curse > 0 else 100
            
            bless_bar = self._progress_bar(progress.bless_received, target_bless)
            curse_bar = self._progress_bar(progress.curse_received, target_curse)
            
            daily_used = progress.bless_given_today + progress.curse_given_today
            daily_remaining = daily_limit - daily_used
            
            status = "✅" if (bless_pct >= 100 and curse_pct >= 100) else "🔄"
            
            print(f"\n{status} {name}:")
            print(f"   Bless: {bless_bar} {progress.bless_received}/{target_bless}")
            print(f"   Curse: {curse_bar} {progress.curse_received}/{target_curse}")
            print(f"   Сегодня выдано: {daily_used}/{daily_limit} (осталось: {daily_remaining})")
            
            if progress.last_action_time:
                print(f"   Последнее действие: {progress.last_action_date} {progress.last_action_time}")
        
        print("\n" + "="*70)
        
        # Общая статистика
        total_bless = sum(acc.bless_received for acc in self.accounts.values())
        total_curse = sum(acc.curse_received for acc in self.accounts.values())
        total_target = len(self.accounts) * (target_bless + target_curse)
        total_done = total_bless + total_curse
        
        print(f"📈 Общий прогресс: {total_done}/{total_target} ({total_done/total_target*100:.1f}%)")
        print("="*70 + "\n")
    
    def _progress_bar(self, current: int, target: int, width: int = 20) -> str:
        """Создать текстовый прогресс-бар"""
        if target == 0:
            return "█" * width
        
        filled = int(width * min(current / target, 1.0))
        empty = width - filled
        return "█" * filled + "░" * empty
    
    def get_summary(self) -> Dict[str, Any]:
        """Получить сводку состояния"""
        target_bless = self.settings["target_bless"]
        target_curse = self.settings["target_curse"]
        
        completed = 0
        in_progress = 0
        
        for name, progress in self.accounts.items():
            if (progress.bless_received >= target_bless and 
                progress.curse_received >= target_curse):
                completed += 1
            else:
                in_progress += 1
        
        return {
            "total_accounts": len(self.accounts),
            "completed": completed,
            "in_progress": in_progress,
            "target_bless": target_bless,
            "target_curse": target_curse,
            "daily_limit": self.settings["daily_limit_per_account"]
        }
    
    def update_settings(self, **kwargs) -> None:
        """Обновить настройки"""
        for key, value in kwargs.items():
            if key in self.settings:
                self.settings[key] = value
                logger.info(f"Setting updated: {key} = {value}")
        self._save_state()

