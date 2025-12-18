"""
Discord RPA Main Orchestrator
Гибкая автоматизация Discord с разными режимами работы

Режимы:
- chain: Паровозик - каждый аккаунт кидает на следующего
- smart: Умный - автоматически определяет кому нужны bless/curse
- target: Все аккаунты кидают на одну цель
- manual: Ручной список пар из pairs.json
"""
import asyncio
import argparse
import json
import random
import signal
import sys
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field

from src.adspower_api import AdsPowerAPI
from src.discord_automation import DiscordAutomation, TimingConfig
from src.account_manager import AccountManager
from src.state_manager import StateManager
from src.logger_config import setup_logger

logger = setup_logger("RitualRPA", log_to_file=True)


# ============================================================================
# CONFIGURATION CLASSES
# ============================================================================

@dataclass
class DelayConfig:
    """Настройки задержек"""
    between_commands_min: int = 30
    between_commands_max: int = 90
    between_accounts_min: int = 300
    between_accounts_max: int = 600
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DelayConfig":
        """Create from dictionary with defaults."""
        return cls(
            between_commands_min=data.get("between_commands_min", 30),
            between_commands_max=data.get("between_commands_max", 90),
            between_accounts_min=data.get("between_accounts_min", 300),
            between_accounts_max=data.get("between_accounts_max", 600)
        )


@dataclass 
class LimitsConfig:
    """Настройки лимитов"""
    enabled: bool = True
    daily_limit_per_account: int = 5
    target_bless: int = 10
    target_curse: int = 10
    max_actions_per_session: int = 20
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LimitsConfig":
        """Create from dictionary with defaults."""
        return cls(
            enabled=data.get("enabled", True),
            daily_limit_per_account=data.get("daily_limit_per_account", 5),
            target_bless=data.get("target_bless", 10),
            target_curse=data.get("target_curse", 10),
            max_actions_per_session=data.get("max_actions_per_session", 20)
        )



@dataclass
class ParallelConfig:
    """Настройки параллельного выполнения."""
    enabled: bool = False
    max_workers: int = 2  # По умолчанию 2-3 профиля параллельно

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ParallelConfig":
        """Create from dictionary with defaults."""
        enabled = data.get("enabled", False)
        # Если параллельность включена, но max_workers не указан, используем 2
        max_workers = data.get("max_workers", 2 if enabled else 1)
        # Ограничиваем максимум 5 для безопасности
        max_workers = min(max_workers, 5)
        return cls(
            enabled=enabled,
            max_workers=max_workers
        )


@dataclass
class BatchModeConfig:
    """Настройки пакетного режима (множественные действия с одного профиля)."""
    enabled: bool = True  # По умолчанию включено - группируем действия по профилю
    max_actions_per_session: int = 10  # Максимум действий в одной сессии браузера
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BatchModeConfig":
        """Create from dictionary with defaults."""
        return cls(
            enabled=data.get("enabled", True),
            max_actions_per_session=data.get("max_actions_per_session", 10)
        )


@dataclass
class RandomPauseConfig:
    """Настройки случайных пауз"""
    enabled: bool = True
    chance: float = 0.2
    min_seconds: int = 60
    max_seconds: int = 180
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RandomPauseConfig":
        """Create from dictionary with defaults."""
        return cls(
            enabled=data.get("enabled", True),
            chance=data.get("chance", 0.2),
            min_seconds=data.get("min_seconds", 60),
            max_seconds=data.get("max_seconds", 180)
        )


@dataclass
class ProfileIdentifier:
    """Идентификатор профиля AdsPower для отслеживания"""
    profile_id: Optional[str] = None
    serial_number: Optional[int] = None
    display_name: str = ""
    
    def __hash__(self):
        return hash((self.profile_id, self.serial_number))
    
    def __eq__(self, other):
        if not isinstance(other, ProfileIdentifier):
            return False
        return self.profile_id == other.profile_id and self.serial_number == other.serial_number
    
    @classmethod
    def from_adspower_id(cls, adspower_id: str, name: str = "") -> "ProfileIdentifier":
        """Create from adspower_id string."""
        is_serial = adspower_id.isdigit() if adspower_id else False
        serial_number = int(adspower_id) if is_serial else None
        profile_id = None if is_serial else adspower_id
        display = f"#{adspower_id}" if is_serial else adspower_id
        
        return cls(
            profile_id=profile_id,
            serial_number=serial_number,
            display_name=f"{name} ({display})" if name else display
        )


# ============================================================================
# SHUTDOWN HANDLER
# ============================================================================

@dataclass
class ShutdownHandler:
    """Обработчик graceful shutdown"""
    adspower: Optional[AdsPowerAPI] = None
    active_profiles: List[ProfileIdentifier] = field(default_factory=list)
    is_shutting_down: bool = False
    
    def register_profile(self, profile: ProfileIdentifier) -> None:
        if profile and profile not in self.active_profiles:
            self.active_profiles.append(profile)
    
    def unregister_profile(self, profile: ProfileIdentifier) -> None:
        if profile in self.active_profiles:
            self.active_profiles.remove(profile)
    
    async def cleanup(self) -> None:
        if self.is_shutting_down:
            return
        self.is_shutting_down = True
        
        if not self.adspower or not self.active_profiles:
            return
        
        print("\n🛑 Завершение работы - закрываю браузеры...")
        for profile in self.active_profiles.copy():
            try:
                print(f"  ⏳ Останавливаю: {profile.display_name}")
                await self.adspower.stop_browser_async(
                    profile_id=profile.profile_id,
                    serial_number=profile.serial_number
                )
                self.active_profiles.remove(profile)
            except Exception as e:
                print(f"  ⚠️ Ошибка: {e}")
        
        await self.adspower.close()
        print("✅ Все браузеры закрыты")


shutdown_handler = ShutdownHandler()


# ============================================================================
# CONFIG LOADERS
# ============================================================================

def load_delay_config(account_mgr: AccountManager) -> DelayConfig:
    """Загрузить настройки задержек."""
    delays = account_mgr.get_config_value("delays", {})
    preset_name = delays.get("preset", "safe")
    
    # Выбираем источник настроек
    if preset_name == "custom":
        source = delays.get("custom", {})
    else:
        presets = delays.get("presets", {})
        source = presets.get(preset_name, {})
    
    return DelayConfig.from_dict(source)


def load_limits_config(account_mgr: AccountManager) -> LimitsConfig:
    """Загрузить настройки лимитов."""
    return LimitsConfig.from_dict(account_mgr.get_config_value("limits", {}))


def load_pause_config(account_mgr: AccountManager) -> RandomPauseConfig:
    """Загрузить настройки случайных пауз."""
    return RandomPauseConfig.from_dict(account_mgr.get_config_value("random_pauses", {}))


def load_timing_config(account_mgr: AccountManager) -> TimingConfig:
    """Загрузить настройки тайминга печати."""
    timing = account_mgr.get_config_value("timing", {})
    return TimingConfig(
        typing_delay_min=timing.get("typing_delay_min", 80),
        typing_delay_max=timing.get("typing_delay_max", 200),
        action_delay_min=timing.get("action_delay_min", 500),
        action_delay_max=timing.get("action_delay_max", 1500),
        autocomplete_wait=timing.get("autocomplete_wait", 3.0),
        command_submit_wait=timing.get("command_submit_wait", 4.0),
        bot_response_timeout=timing.get("bot_response_timeout", 8.0)
    )


def load_parallel_config(account_mgr: AccountManager) -> ParallelConfig:
    """Загрузить настройки параллельного выполнения."""
    parallel_data = account_mgr.get_config_value("parallel", {})
    return ParallelConfig.from_dict(parallel_data)


def load_batch_mode_config(account_mgr: AccountManager) -> BatchModeConfig:
    """Загрузить настройки пакетного режима."""
    batch_data = account_mgr.get_config_value("batch_mode", {})
    return BatchModeConfig.from_dict(batch_data)


# ============================================================================
# PAIR GENERATORS
# ============================================================================

def generate_chain_pairs(
    accounts: List[Dict], 
    both_actions: bool = True,
    account_mgr: Optional[AccountManager] = None
) -> List[Dict]:
    """
    Режим CHAIN: каждый аккаунт кидает на следующего по кругу.
    
    Args:
        accounts: Список аккаунтов
        both_actions: Если True, каждый аккаунт делает и bless, и curse
        account_mgr: Менеджер аккаунтов для проверки заблокированных
        
    Returns:
        Список пар действий
    """
    pairs = []
    total = len(accounts)
    
    if total < 2:
        return pairs
    
    for i, account in enumerate(accounts):
        next_idx = (i + 1) % total
        target = accounts[next_idx]
        
        # Пропускаем пары где giver или receiver заблокирован
        giver_name = account.get("name", "")
        receiver_name = target.get("name", "")
        giver_id = account.get("adspower_id", "")
        receiver_id = target.get("adspower_id", "")
        
        if account_mgr:
            if account_mgr.is_account_blocked(giver_name, giver_id):
                continue
            if account_mgr.is_account_blocked(receiver_name, receiver_id):
                continue
        
        if both_actions:
            pairs.append({"giver": account, "receiver": target, "action": "bless"})
            pairs.append({"giver": account, "receiver": target, "action": "curse"})
        else:
            action = "bless" if i % 2 == 0 else "curse"
            pairs.append({"giver": account, "receiver": target, "action": action})
    
    return pairs


def generate_target_pairs(
    accounts: List[Dict], 
    target_username: str,
    account_mgr: Optional[AccountManager] = None
) -> List[Dict]:
    """
    Режим TARGET: все аккаунты кидают на одну цель.
    
    Args:
        accounts: Список аккаунтов
        target_username: Discord username цели
        account_mgr: Менеджер аккаунтов для проверки заблокированных
        
    Returns:
        Список пар действий
    """
    target = {"name": f"Target: {target_username}", "discord_username": target_username}
    
    pairs = []
    for account in accounts:
        # Пропускаем заблокированные аккаунты
        account_name = account.get("name", "")
        account_id = account.get("adspower_id", "")
        
        if account_mgr and account_mgr.is_account_blocked(account_name, account_id):
            continue
        
        pairs.append({"giver": account, "receiver": target, "action": "bless"})
        pairs.append({"giver": account, "receiver": target, "action": "curse"})
    
    return pairs


def generate_smart_pairs(
    accounts: List[Dict], 
    state_mgr: StateManager, 
    max_actions: int,
    account_mgr: Optional[AccountManager] = None
) -> List[Dict]:
    """Режим SMART: автоматически определяет кому нужны bless/curse."""
    return state_mgr.get_optimal_pairs(accounts, max_actions=max_actions, account_mgr=account_mgr)


def load_manual_pairs(
    accounts: List[Dict], 
    account_mgr: Optional[AccountManager] = None
) -> List[Dict]:
    """
    Режим MANUAL: загружает пары из файла pairs.json.
    
    Args:
        accounts: Список аккаунтов
        account_mgr: Менеджер аккаунтов для проверки заблокированных
        
    Returns:
        Список пар действий
    """
    try:
        with open("pairs.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        
        accounts_by_name = {acc["name"]: acc for acc in accounts}
        pairs = []
        
        for pair in data.get("pairs", []):
            giver_name = pair.get("giver")
            receiver_name = pair.get("receiver")
            action = pair.get("action", "bless")
            
            if giver_name not in accounts_by_name:
                print(f"⚠️ Аккаунт не найден: {giver_name}")
                continue
            
            giver = accounts_by_name[giver_name]
            receiver = accounts_by_name.get(receiver_name, {
                "name": receiver_name,
                "discord_username": pair.get("discord_username", receiver_name)
            })
            
            # Пропускаем пары где giver или receiver заблокирован
            giver_id = giver.get("adspower_id", "")
            receiver_id = receiver.get("adspower_id", "")
            
            if account_mgr:
                if account_mgr.is_account_blocked(giver_name, giver_id):
                    print(f"⚠️ Пропущена пара: {giver_name} заблокирован")
                    continue
                if account_mgr.is_account_blocked(receiver_name, receiver_id):
                    print(f"⚠️ Пропущена пара: {receiver_name} заблокирован")
                    continue
            
            pairs.append({"giver": giver, "receiver": receiver, "action": action})
        
        return pairs
        
    except FileNotFoundError:
        print("❌ Файл pairs.json не найден")
        print('   Создайте файл со структурой: {"pairs": [...]}')
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка в pairs.json: {e}")
        return []


# ============================================================================
# HELPERS
# ============================================================================

def get_random_delay(min_val: int, max_val: int) -> float:
    """Случайная задержка с небольшой вариацией."""
    base = random.uniform(min_val, max_val)
    variation = base * random.uniform(-0.2, 0.2)
    return max(1, base + variation)


def group_pairs_by_giver(pairs: List[Dict]) -> List[Tuple[Dict, List[Dict]]]:
    """
    Группировать пары по отдающему аккаунту.
    Возвращает список кортежей (giver, list_of_actions).
    Каждое действие в list_of_actions содержит receiver и action.
    """
    groups = {}
    
    # Сохраняем порядок появления
    order = []
    
    for pair in pairs:
        giver = pair["giver"]
        giver_name = giver.get("name")
        
        if giver_name not in groups:
            groups[giver_name] = {
                "giver": giver,
                "actions": []
            }
            order.append(giver_name)
        
        groups[giver_name]["actions"].append({
            "receiver": pair["receiver"],
            "action": pair["action"]
        })
    
    return [(groups[name]["giver"], groups[name]["actions"]) for name in order]


async def maybe_random_pause(pause_config: RandomPauseConfig) -> None:
    """Случайная пауза для имитации человека."""
    if pause_config.enabled and random.random() < pause_config.chance:
        pause = random.uniform(pause_config.min_seconds, pause_config.max_seconds)
        print(f"\n☕ Случайная пауза {pause:.0f} сек...")
        await asyncio.sleep(pause)


async def countdown_delay(seconds: float, message: str = "Ожидание") -> None:
    """Задержка с отображением обратного отсчёта."""
    remaining = int(seconds)
    print(f"\n⏳ {message}: {remaining} сек ({remaining/60:.1f} мин)")
    
    update_interval = 30
    
    while remaining > 0 and not shutdown_handler.is_shutting_down:
        sleep_time = min(update_interval, remaining)
        await asyncio.sleep(sleep_time)
        remaining -= sleep_time
        
        if remaining > 0:
            mins, secs = divmod(remaining, 60)
            time_str = f"{mins} мин {secs} сек" if mins > 0 else f"{secs} сек"
            print(f"   ⏳ Осталось: {time_str}...")


def print_action_header(action_type: str, giver: Dict, receiver: Dict, profile_display: str) -> None:
    """Вывести заголовок действия."""
    emoji = "✨" if action_type == "bless" else "💀"
    giver_name = giver.get("name", "Unknown")
    receiver_name = receiver.get("name", "Unknown")
    receiver_discord = receiver.get("discord_username", "?")
    
    print(f"\n{'='*60}")
    print(f"{emoji} {action_type.upper()}: {giver_name} → {receiver_name}")
    print(f"   Профиль: {profile_display}")
    print(f"   Цель: @{receiver_discord}")
    print(f"{'='*60}")


# ============================================================================
# ACTION EXECUTOR
# ============================================================================


async def execute_giver_batch(
    adspower: AdsPowerAPI,
    giver: Dict[str, Any],
    actions: List[Dict],
    channel_url: str,
    timing_config: TimingConfig,
    delays: DelayConfig,
    account_mgr: Optional[AccountManager] = None,
    state_mgr: Optional[StateManager] = None
) -> Tuple[int, int]:
    """
    Выполнить пакет действий для одного аккаунта (одна сессия браузера).
    Возвращает (completed, failed).
    """
    completed = 0
    failed = 0
    
    giver_name = giver.get("name", "Unknown")
    adspower_id = giver.get("adspower_id", "")
    
    # Проверка на блокировку giver
    if account_mgr and account_mgr.is_account_blocked(giver_name, adspower_id):
        print(f"🚫 Аккаунт {giver_name} заблокирован, пропускаем {len(actions)} действий")
        return 0, 0 # Не считаем как ошибку, просто пропуск
        
    # Создаём идентификатор профиля
    profile = ProfileIdentifier.from_adspower_id(adspower_id, giver_name)
    
    if shutdown_handler.is_shutting_down:
        return 0, 0
        
    # Запуск браузера
    print(f"\n🚀 Запуск браузера для {giver_name} ({len(actions)} действий)...")
    browser_info = await adspower.start_browser(
        profile_id=profile.profile_id,
        serial_number=profile.serial_number
    )
    
    if not browser_info:
        print(f"❌ Не удалось запустить браузер для {giver_name}")
        # Записываем все как ошибки
        for act in actions:
            if state_mgr:
                state_mgr.record_action(giver_name, act["receiver"].get("name"), act["action"], False)
        return 0, len(actions)
        
    shutdown_handler.register_profile(profile)
    
    try:
        print(f"⏳ Инициализация браузера {giver_name}...")
        await asyncio.sleep(5)
        
        # Выполняем действия
        for i, action_data in enumerate(actions):
            if shutdown_handler.is_shutting_down:
                break
                
            receiver = action_data["receiver"]
            action_type = action_data["action"]
            receiver_name = receiver.get("name", "Unknown")
            receiver_discord = receiver.get("discord_username")
            receiver_adspower_id = receiver.get("adspower_id", "")
            
            # Проверка на блокировку receiver
            if account_mgr and account_mgr.is_account_blocked(receiver_name, receiver_adspower_id):
                print(f"🚫 Получатель {receiver_name} заблокирован, пропускаем действие")
                continue
                
            profile_display = f"#{adspower_id}" if adspower_id.isdigit() else adspower_id
            print_action_header(action_type, giver, receiver, profile_display)
            
            # Выполнение действия
            success = await _execute_discord_action(
                browser_info, 
                channel_url, 
                timing_config, 
                action_type, 
                receiver_discord,
                giver_name,
                receiver_name,
                adspower_id,
                giver.get("discord_username"),
                account_mgr,
                state_mgr
            )
            
            if success:
                completed += 1
            else:
                failed += 1
            
            if state_mgr:
                state_mgr.record_action(giver_name, receiver_name, action_type, success)
                
            # Пауза между действиями внутри одного сеанса
            if i < len(actions) - 1 and not shutdown_handler.is_shutting_down:
                delay = get_random_delay(delays.between_commands_min, delays.between_commands_max)
                print(f"⏳ Пауза между командами {giver_name}: {delay:.1f} сек...")
                await asyncio.sleep(delay)
                
    finally:
        # Закрытие браузера
        print(f"\n🛑 Закрываю браузер {giver_name}...")
        try:
            await adspower.stop_browser_async(
                profile_id=profile.profile_id,
                serial_number=profile.serial_number
            )
            shutdown_handler.unregister_profile(profile)
        except Exception as e:
            print(f"⚠️ Ошибка закрытия: {e}")
            
    return completed, failed


async def execute_action(
    adspower: AdsPowerAPI,
    giver: Dict[str, Any],
    receiver: Dict[str, Any],
    action_type: str,
    channel_url: str,
    timing_config: TimingConfig,
    account_mgr: Optional[AccountManager] = None,
    state_mgr: Optional[StateManager] = None
) -> bool:
    """Выполнить одно действие (bless или curse)."""
    
    giver_name = giver.get("name", "Unknown")
    receiver_name = receiver.get("name", "Unknown")
    receiver_discord = receiver.get("discord_username")
    adspower_id = giver.get("adspower_id", "")
    receiver_adspower_id = receiver.get("adspower_id", "")
    
    # Проверка на заблокированный giver
    if account_mgr and account_mgr.is_account_blocked(giver_name, adspower_id):
        blocked_data = account_mgr._blocked_accounts.get(giver_name, {})
        reason = blocked_data.get("reason", "Неизвестная причина")
        print(f"🚫 Аккаунт {giver_name} (giver) заблокирован: {reason}")
        print(f"   Пропускаю это действие...")
        return False
    
    # Проверка на заблокированный receiver
    if account_mgr and account_mgr.is_account_blocked(receiver_name, receiver_adspower_id):
        blocked_data = account_mgr._blocked_accounts.get(receiver_name, {})
        reason = blocked_data.get("reason", "Неизвестная причина")
        print(f"🚫 Аккаунт {receiver_name} (receiver) заблокирован: {reason}")
        print(f"   Пропускаю это действие...")
        return False
    
    # Создаём идентификатор профиля
    profile = ProfileIdentifier.from_adspower_id(adspower_id, giver_name)
    profile_display = f"#{adspower_id}" if adspower_id.isdigit() else adspower_id
    
    print_action_header(action_type, giver, receiver, profile_display)
    
    # Валидация
    if not adspower_id or not receiver_discord:
        print(f"❌ Нет данных: adspower_id={adspower_id}, receiver={receiver_discord}")
        if state_mgr:
            state_mgr.record_action(giver_name, receiver_name, action_type, False)
        return False
    
    if shutdown_handler.is_shutting_down:
        return False
    
    # Запуск браузера
    print(f"\n🚀 Запуск браузера...")
    browser_info = await adspower.start_browser(
        profile_id=profile.profile_id,
        serial_number=profile.serial_number
    )
    
    if not browser_info:
        print(f"❌ Не удалось запустить браузер")
        if state_mgr:
            state_mgr.record_action(giver_name, receiver_name, action_type, False)
        return False
    
    shutdown_handler.register_profile(profile)
    
    print("⏳ Инициализация браузера...")
    await asyncio.sleep(5)
    
    success = await _execute_discord_action(
        browser_info, 
        channel_url, 
        timing_config, 
        action_type, 
        receiver_discord,
        giver_name,
        receiver_name,
        adspower_id,
        giver.get("discord_username"),
        account_mgr,
        state_mgr
    )
    
    # Закрытие браузера
    print(f"\n🛑 Закрываю браузер...")
    try:
        await adspower.stop_browser_async(
            profile_id=profile.profile_id,
            serial_number=profile.serial_number
        )
        shutdown_handler.unregister_profile(profile)
    except Exception as e:
        print(f"⚠️ Ошибка закрытия: {e}")
    
    if state_mgr:
        state_mgr.record_action(giver_name, receiver_name, action_type, success)
    
    return success


async def _execute_discord_action(
    browser_info: Dict,
    channel_url: str,
    timing_config: TimingConfig,
    action_type: str,
    target_discord: str,
    giver_name: str,
    receiver_name: str,
    adspower_id: str,
    discord_username: Optional[str],
    account_mgr: Optional[AccountManager],
    state_mgr: Optional[StateManager]
) -> bool:
    """Выполнить Discord команду в браузере."""
    try:
        cdp_url = browser_info.get("cdp_url") or browser_info.get("ws_url")
        
        async with DiscordAutomation(cdp_url, timing=timing_config) as discord:
            if not discord.is_connected:
                print(f"❌ Не удалось подключиться к браузеру")
                return False
            
            if shutdown_handler.is_shutting_down:
                return False
            
            # Проверяем авторизацию сразу после подключения
            print(f"\n🔍 Проверка авторизации Discord...")
            is_logged_in = await discord.verify_discord_login()
            
            if not is_logged_in:
                print(f"❌ Аккаунт не авторизован в Discord!")
                
                # Блокируем аккаунт как неавторизованный
                if account_mgr:
                    print(f"   🔒 Блокирую аккаунт {giver_name}...")
                    account_mgr.block_account(
                        account_name=giver_name,
                        adspower_id=adspower_id,
                        reason="Аккаунт не авторизован в Discord",
                        discord_username=discord_username,
                        block_type="unauthorized"
                    )
                else:
                    print(f"   ⚠️ account_mgr не передан, блокировка не выполнена")
                return False
            
            # Навигация
            print(f"\n🔗 Переход в канал Discord...")
            channel_loaded = await discord.navigate_to_channel(channel_url)
            
            if not channel_loaded:
                print(f"❌ Не удалось открыть канал")
                
                # Проверяем конкретную ошибку доступа
                access_error = await discord._check_channel_access()
                
                if access_error:
                    # Есть конкретное сообщение об ошибке доступа - блокируем
                    print(f"   🚫 Обнаружена проблема с доступом: {access_error}")
                    
                    if account_mgr:
                        print(f"   🔒 Блокирую аккаунт {giver_name}...")
                        account_mgr.block_account(
                            account_name=giver_name,
                            adspower_id=adspower_id,
                            reason=f"Нет доступа к каналу: {access_error[:100]}",
                            discord_username=discord_username,
                            block_type="channel"
                        )
                    else:
                        print(f"   ⚠️ account_mgr не передан, блокировка не выполнена")
                else:
                    # Проверяем, может ли быть проблема с доступом (нет input поля)
                    # Если канал загрузился, но нет input - это проблема доступа
                    try:
                        # Проверяем наличие input поля для сообщений
                        input_selector = 'div[role="textbox"][aria-label*="Message"], div[role="textbox"][data-slate-editor="true"], div[role="textbox"]'
                        input_elem = await discord.page.query_selector(input_selector)
                        
                        if not input_elem:
                            # Канал открыт, но нет доступа к отправке сообщений
                            print(f"   🚫 Канал открыт, но нет доступа к отправке сообщений")
                            
                            if account_mgr:
                                print(f"   🔒 Блокирую аккаунт {giver_name}...")
                                account_mgr.block_account(
                                    account_name=giver_name,
                                    adspower_id=adspower_id,
                                    reason="Нет доступа к отправке сообщений в канале",
                                    discord_username=discord_username,
                                    block_type="channel"
                                )
                            else:
                                print(f"   ⚠️ account_mgr не передан, блокировка не выполнена")
                        else:
                            # Есть input, но канал не загрузился полностью - возможно временная проблема
                            print(f"   ⚠️ Не удалось открыть канал, но конкретная ошибка доступа не обнаружена")
                            print(f"   💡 Возможно временная проблема или медленная загрузка")
                            # Не блокируем, если есть input поле - значит доступ есть
                    except Exception as e:
                        # Ошибка при проверке - блокируем для безопасности
                        print(f"   🚫 Ошибка при проверке доступа: {e}")
                        
                        if account_mgr:
                            print(f"   🔒 Блокирую аккаунт {giver_name}...")
                            account_mgr.block_account(
                                account_name=giver_name,
                                adspower_id=adspower_id,
                                reason=f"Проблема с доступом к каналу: {str(e)[:100]}",
                                discord_username=discord_username,
                                block_type="channel"
                            )
                        else:
                            print(f"   ⚠️ account_mgr не передан, блокировка не выполнена")
                
                return False
            
            # Выполнение команды
            print(f"\n⚡ Выполняю /{action_type} на @{target_discord}...")
            
            if action_type == "bless":
                success = await discord.execute_bless(target_discord)
            elif action_type == "curse":
                success = await discord.execute_curse(target_discord)
            else:
                success = False
            
            if success:
                print(f"✅ {action_type.capitalize()} успешно!")
            else:
                print(f"❌ {action_type.capitalize()} не удался")
            
            await asyncio.sleep(3)
            return success
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


# ============================================================================
# SESSION RUNNER
# ============================================================================

async def run_session(
    adspower: AdsPowerAPI,
    account_mgr: AccountManager,
    state_mgr: StateManager,
    mode: str,
    channel_url: str,
    delays: DelayConfig,
    limits: LimitsConfig,
    pauses: RandomPauseConfig,
    timing: TimingConfig,
    parallel: ParallelConfig,
    batch_mode: BatchModeConfig,
    max_actions: Optional[int] = None
) -> None:
    """Запуск сессии автоматизации."""
    
    accounts = account_mgr.get_config_value("accounts", [])
    modes_config = account_mgr.get_config_value("modes", {})
    
    # Фильтруем заблокированные аккаунты
    accounts = account_mgr.filter_blocked_accounts(accounts)
    
    if not accounts:
        print("\n⚠️ Все аккаунты заблокированы или отсутствуют!")
        return
    
    # Генерируем пары
    pairs = _generate_pairs_for_mode(mode, accounts, modes_config, state_mgr, limits, max_actions, account_mgr)
    
    if pairs is None:
        return
    
    if not pairs:
        print("\n✅ Нет действий для выполнения!")
        return
    
    # Применяем лимит
    max_act = max_actions or limits.max_actions_per_session
    if limits.enabled and len(pairs) > max_act:
        print(f"\n⚠️ Ограничено до {max_act} действий (из {len(pairs)})")
        pairs = pairs[:max_act]
    
    _print_execution_plan(pairs, delays)
    
    # Выполнение
    completed, failed = await _execute_pairs(
        pairs, adspower, channel_url, timing, delays, pauses, parallel, batch_mode, account_mgr, state_mgr, mode
    )
    
    _print_session_summary(completed, failed)


def _generate_pairs_for_mode(
    mode: str,
    accounts: List[Dict],
    modes_config: Dict,
    state_mgr: StateManager,
    limits: LimitsConfig,
    max_actions: Optional[int],
    account_mgr: Optional[AccountManager] = None
) -> Optional[List[Dict]]:
    """Сгенерировать пары в зависимости от режима."""
    print(f"\n📋 Режим: {mode.upper()}")
    
    if mode == "chain":
        chain_config = modes_config.get("chain", {})
        both_actions = chain_config.get("both_bless_and_curse", True)
        print(f"   Паровозик: каждый → следующий")
        return generate_chain_pairs(accounts, both_actions, account_mgr=account_mgr)
    
    elif mode == "target":
        target_config = modes_config.get("target", {})
        target_user = target_config.get("target_username", "")
        if not target_user:
            print("❌ Не указан target_username в конфиге")
            return None
        print(f"   Все аккаунты → @{target_user}")
        return generate_target_pairs(accounts, target_user, account_mgr=account_mgr)
    
    elif mode == "smart":
        state_mgr.update_settings(
            daily_limit_per_account=limits.daily_limit_per_account,
            target_bless=limits.target_bless,
            target_curse=limits.target_curse
        )
        max_act = max_actions or limits.max_actions_per_session
        print(f"   Умный режим: автовыбор кому нужны bless/curse")
        return generate_smart_pairs(accounts, state_mgr, max_act, account_mgr=account_mgr)
    
    elif mode == "manual":
        print(f"   Ручной режим: из pairs.json")
        return load_manual_pairs(accounts, account_mgr=account_mgr)
    
    else:
        print(f"❌ Неизвестный режим: {mode}")
        return None


def _print_execution_plan(pairs: List[Dict], delays: DelayConfig) -> None:
    """Вывести план выполнения."""
    print(f"\n📝 Запланировано: {len(pairs)} действий")
    print("-"*50)
    
    for i, pair in enumerate(pairs, 1):
        g = pair["giver"]["name"]
        r = pair["receiver"].get("name", pair["receiver"].get("discord_username", "?"))
        a = pair["action"]
        emoji = "✨" if a == "bless" else "💀"
        print(f"   {i:2}. {emoji} {g} → {a} → {r}")
    
    print("-"*50)
    
    avg_delay = (delays.between_commands_min + delays.between_commands_max) / 2
    estimated_time = len(pairs) * (avg_delay + 60) / 60
    print(f"\n⏱️ Примерное время: {estimated_time:.0f} мин")


async def _execute_pairs(
    pairs: List[Dict],
    adspower: AdsPowerAPI,
    channel_url: str,
    timing: TimingConfig,
    delays: DelayConfig,
    pauses: RandomPauseConfig,
    parallel: ParallelConfig,
    batch_mode: BatchModeConfig,
    account_mgr: AccountManager,
    state_mgr: StateManager,
    mode: str
) -> tuple:
    """Выполнить все пары действий (параллельно или последовательно)."""
    
    # 1. Группируем по giver'у для оптимизации (один запуск браузера = много действий)
    if batch_mode.enabled:
        groups = group_pairs_by_giver(pairs)
        # Ограничиваем количество действий на сессию
        if batch_mode.max_actions_per_session > 0:
            limited_groups = []
            for giver, actions in groups:
                if len(actions) > batch_mode.max_actions_per_session:
                    # Разбиваем на несколько групп
                    for i in range(0, len(actions), batch_mode.max_actions_per_session):
                        limited_groups.append((giver, actions[i:i + batch_mode.max_actions_per_session]))
                else:
                    limited_groups.append((giver, actions))
            groups = limited_groups
        print(f"\n🧩 Сгруппировано в {len(groups)} сессий (запусков браузера).")
        print(f"   📦 Пакетный режим: до {batch_mode.max_actions_per_session} действий на сессию")
    else:
        # Если пакетный режим выключен, каждое действие в отдельной сессии
        groups = [(pair["giver"], [{"receiver": pair["receiver"], "action": pair["action"]}]) for pair in pairs]
        print(f"\n🧩 Режим без группировки: {len(groups)} сессий (по 1 действию на сессию)")
    
    if parallel.enabled and parallel.max_workers > 1:
        print(f"🚀 Включен параллельный режим (max {parallel.max_workers} профилей одновременно)")
        return await _execute_parallel(
            groups, adspower, channel_url, timing, delays, parallel, account_mgr, state_mgr
        )
    else:
        print(f"🐢 Последовательный режим выполнения")
        return await _execute_sequential(
             groups, adspower, channel_url, timing, delays, pauses, account_mgr, state_mgr
        )


async def _execute_sequential(
    groups: List[Tuple[Dict, List[Dict]]],
    adspower: AdsPowerAPI,
    channel_url: str,
    timing: TimingConfig,
    delays: DelayConfig,
    pauses: RandomPauseConfig,
    account_mgr: AccountManager,
    state_mgr: StateManager
) -> Tuple[int, int]:
    """Последовательное выполнение групп."""
    total_completed = 0
    total_failed = 0
    
    for i, (giver, actions) in enumerate(groups):
        if shutdown_handler.is_shutting_down:
            print("\n⚠️ Прерывание...")
            break
            
        print(f"\n{'='*60}")
        print(f"👤 Сессия {i+1}/{len(groups)}: {giver.get('name')} ({len(actions)} действий)")
        print(f"{'='*60}")
        
        # Выполняем пакет действий
        c, f = await execute_giver_batch(
            adspower=adspower,
            giver=giver, 
            actions=actions,
            channel_url=channel_url,
            timing_config=timing,
            delays=delays,
            account_mgr=account_mgr,
            state_mgr=state_mgr
        )
        total_completed += c
        total_failed += f
        
        # Пауза между аккаунтами
        if i < len(groups) - 1 and not shutdown_handler.is_shutting_down:
            delay = get_random_delay(delays.between_accounts_min, delays.between_accounts_max)
            await countdown_delay(delay, "Смена аккаунта")
            await maybe_random_pause(pauses)
            
    return total_completed, total_failed


async def _execute_parallel(
    groups: List[Tuple[Dict, List[Dict]]],
    adspower: AdsPowerAPI,
    channel_url: str,
    timing: TimingConfig,
    delays: DelayConfig,
    parallel: ParallelConfig,
    account_mgr: AccountManager,
    state_mgr: StateManager
) -> Tuple[int, int]:
    """Параллельное выполнение групп."""
    semaphore = asyncio.Semaphore(parallel.max_workers)
    total_completed = 0
    total_failed = 0
    completed_lock = asyncio.Lock()  # Для потокобезопасного обновления счётчиков
    
    async def worker(giver, actions, worker_id: int):
        nonlocal total_completed, total_failed
        async with semaphore:
            if shutdown_handler.is_shutting_down:
                return
                
            giver_name = giver.get('name', 'Unknown')
            print(f"\n{'='*60}")
            print(f"🚀 [Поток {worker_id}] Старт для {giver_name} ({len(actions)} действий)")
            print(f"{'='*60}")
            
            try:
                c, f = await execute_giver_batch(
                    adspower=adspower,
                    giver=giver, 
                    actions=actions,
                    channel_url=channel_url,
                    timing_config=timing,
                    delays=delays,
                    account_mgr=account_mgr,
                    state_mgr=state_mgr
                )
                
                async with completed_lock:
                    total_completed += c
                    total_failed += f
                
                print(f"\n✅ [Поток {worker_id}] Завершено для {giver_name}: {c} успешно, {f} ошибок")
            except Exception as e:
                print(f"\n❌ [Поток {worker_id}] Ошибка для {giver_name}: {e}")
                async with completed_lock:
                    total_failed += len(actions)
            
    tasks = [worker(g, a, i+1) for i, (g, a) in enumerate(groups)]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    return total_completed, total_failed


def _print_session_summary(completed: int, failed: int) -> None:
    """Вывести итоги сессии."""
    print(f"\n{'='*60}")
    print(f"📊 ИТОГИ СЕССИИ")
    print(f"{'='*60}")
    print(f"✅ Успешно: {completed}")
    print(f"❌ Ошибок: {failed}")
    
    total = completed + failed
    if total > 0:
        print(f"📈 Успешность: {completed/total*100:.0f}%")


# ============================================================================
# MAIN
# ============================================================================

async def main_async(args) -> None:
    """Основная функция."""
    
    print("="*60)
    print("🤖 Discord RPA Automation")
    print("="*60 + "\n")
    
    account_mgr = None
    state_mgr = None
    adspower = None
    
    try:
        # Загрузка конфигурации
        account_mgr = AccountManager("config.json")
        if not account_mgr.load_config():
            print("❌ Не удалось загрузить config.json")
            return
        
        state_mgr = StateManager("state.json")
        
        # Загрузка всех настроек
        mode = args.mode or account_mgr.get_config_value("mode", "chain")
        delays = load_delay_config(account_mgr)
        limits = load_limits_config(account_mgr)
        pauses = load_pause_config(account_mgr)
        timing = load_timing_config(account_mgr)
        parallel = load_parallel_config(account_mgr)
        batch_mode = load_batch_mode_config(account_mgr)
        channel_url = account_mgr.get_config_value("discord_channel_url")
        
        if not channel_url:
            print("❌ discord_channel_url не настроен")
            return
        
        # Показываем настройки
        print(f"⚙️ Настройки:")
        print(f"   Режим: {mode}")
        print(f"   Задержки: {delays.between_commands_min}-{delays.between_commands_max}с")
        print(f"   Лимиты: {'включены' if limits.enabled else 'выключены'}")
        print(f"   Случайные паузы: {'включены' if pauses.enabled else 'выключены'}")
        if batch_mode.enabled:
            print(f"   Пакетный режим: до {batch_mode.max_actions_per_session} действий на профиль")
        if parallel.enabled:
            print(f"   Параллельность: {parallel.max_workers} профилей одновременно")
        
        # Режим статуса
        if args.status:
            state_mgr.print_progress_report()
            return
        
        # Подключение к AdsPower
        api_url = account_mgr.get_config_value("adspower_api_url", "http://localhost:50325")
        adspower = AdsPowerAPI(api_url)
        shutdown_handler.adspower = adspower
        
        print("\n🔍 Проверка AdsPower...")
        if not adspower.check_connection():
            print("❌ AdsPower не доступен")
            return
        print("✅ AdsPower подключен\n")
        
        # Запуск сессии
        await run_session(
            adspower=adspower,
            account_mgr=account_mgr,
            state_mgr=state_mgr,
            mode=mode,
            channel_url=channel_url,
            delays=delays,
            limits=limits,
            pauses=pauses,
            timing=timing,
            parallel=parallel,
            batch_mode=batch_mode,
            max_actions=args.limit
        )
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await shutdown_handler.cleanup()
        
        if state_mgr and (args.mode == "smart" or not args.mode):
            state_mgr.print_progress_report()
        
        if adspower:
            try:
                await adspower.close()
            except Exception:
                pass


def handle_sigint(signum, frame):
    """Обработчик Ctrl+C."""
    print("\n\n⚠️ Ctrl+C - завершаю...")
    shutdown_handler.is_shutting_down = True


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Discord RPA Automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Режимы работы:
  chain   - Паровозик: каждый аккаунт → следующий
  smart   - Умный: автовыбор кому нужны bless/curse
  target  - Все аккаунты → одна цель
  manual  - Ручной список из pairs.json

Примеры:
  python main.py                    # Режим из конфига
  python main.py -m chain           # Режим паровозик
  python main.py -m smart -l 10     # Умный режим, max 10 действий
  python main.py --status           # Показать прогресс
        """
    )
    
    parser.add_argument("-m", "--mode", choices=["chain", "smart", "target", "manual"],
                        help="Режим работы")
    parser.add_argument("-l", "--limit", type=int, help="Макс действий за сессию")
    parser.add_argument("-s", "--status", action="store_true", help="Показать прогресс")
    
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, handle_sigint)
    
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\n\n⚠️ Прервано")
        sys.exit(0)


if __name__ == "__main__":
    main()
