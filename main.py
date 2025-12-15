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
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

from adspower_api import AdsPowerAPI
from discord_automation import DiscordAutomation, TimingConfig
from account_manager import AccountManager
from state_manager import StateManager
from logger_config import setup_logger

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


@dataclass 
class LimitsConfig:
    """Настройки лимитов"""
    enabled: bool = True
    daily_limit_per_account: int = 5
    target_bless: int = 10
    target_curse: int = 10
    max_actions_per_session: int = 20


@dataclass
class RandomPauseConfig:
    """Настройки случайных пауз"""
    enabled: bool = True
    chance: float = 0.2
    min_seconds: int = 60
    max_seconds: int = 180


@dataclass
class ProfileIdentifier:
    """Идентификатор профиля AdsPower"""
    profile_id: Optional[str] = None
    serial_number: Optional[int] = None
    display_name: str = ""
    
    def __hash__(self):
        return hash((self.profile_id, self.serial_number))
    
    def __eq__(self, other):
        if not isinstance(other, ProfileIdentifier):
            return False
        return self.profile_id == other.profile_id and self.serial_number == other.serial_number


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
                display = profile.display_name or profile.profile_id or f"#{profile.serial_number}"
                print(f"  ⏳ Останавливаю: {display}")
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
    """Загрузить настройки задержек"""
    delays = account_mgr.get_config_value("delays", {})
    preset_name = delays.get("preset", "safe")
    
    # Если preset = "custom", используем кастомные настройки
    if preset_name == "custom":
        custom = delays.get("custom", {})
        return DelayConfig(
            between_commands_min=custom.get("between_commands_min", 30),
            between_commands_max=custom.get("between_commands_max", 90),
            between_accounts_min=custom.get("between_accounts_min", 300),
            between_accounts_max=custom.get("between_accounts_max", 600)
        )
    
    # Иначе берём из пресетов
    presets = delays.get("presets", {})
    preset = presets.get(preset_name, {})
    
    return DelayConfig(
        between_commands_min=preset.get("between_commands_min", 30),
        between_commands_max=preset.get("between_commands_max", 90),
        between_accounts_min=preset.get("between_accounts_min", 300),
        between_accounts_max=preset.get("between_accounts_max", 600)
    )


def load_limits_config(account_mgr: AccountManager) -> LimitsConfig:
    """Загрузить настройки лимитов"""
    limits = account_mgr.get_config_value("limits", {})
    return LimitsConfig(
        enabled=limits.get("enabled", True),
        daily_limit_per_account=limits.get("daily_limit_per_account", 5),
        target_bless=limits.get("target_bless", 10),
        target_curse=limits.get("target_curse", 10),
        max_actions_per_session=limits.get("max_actions_per_session", 20)
    )


def load_pause_config(account_mgr: AccountManager) -> RandomPauseConfig:
    """Загрузить настройки случайных пауз"""
    pauses = account_mgr.get_config_value("random_pauses", {})
    return RandomPauseConfig(
        enabled=pauses.get("enabled", True),
        chance=pauses.get("chance", 0.2),
        min_seconds=pauses.get("min_seconds", 60),
        max_seconds=pauses.get("max_seconds", 180)
    )


def load_timing_config(account_mgr: AccountManager) -> TimingConfig:
    """Загрузить настройки тайминга печати"""
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


# ============================================================================
# PAIR GENERATORS (разные режимы)
# ============================================================================

def generate_chain_pairs(accounts: List[Dict], both_actions: bool = True) -> List[Dict]:
    """
    Режим CHAIN (паровозик)
    Каждый аккаунт кидает на следующего по кругу
    """
    pairs = []
    total = len(accounts)
    
    for i, account in enumerate(accounts):
        next_idx = (i + 1) % total
        target = accounts[next_idx]
        
        if both_actions:
            # Сначала bless, потом curse
            pairs.append({
                "giver": account,
                "receiver": target,
                "action": "bless"
            })
            pairs.append({
                "giver": account,
                "receiver": target,
                "action": "curse"
            })
        else:
            # Только одно действие (чередуем)
            action = "bless" if i % 2 == 0 else "curse"
            pairs.append({
                "giver": account,
                "receiver": target,
                "action": action
            })
    
    return pairs


def generate_target_pairs(accounts: List[Dict], target_username: str) -> List[Dict]:
    """
    Режим TARGET
    Все аккаунты кидают на одну указанную цель
    """
    pairs = []
    
    # Создаём фейковый receiver
    target = {
        "name": f"Target: {target_username}",
        "discord_username": target_username
    }
    
    for account in accounts:
        pairs.append({
            "giver": account,
            "receiver": target,
            "action": "bless"
        })
        pairs.append({
            "giver": account,
            "receiver": target,
            "action": "curse"
        })
    
    return pairs


def generate_smart_pairs(accounts: List[Dict], state_mgr: StateManager, 
                         limits: LimitsConfig, max_actions: int) -> List[Dict]:
    """
    Режим SMART
    Автоматически определяет кому нужны bless/curse
    """
    return state_mgr.get_optimal_pairs(accounts, max_actions=max_actions)


def load_manual_pairs(accounts: List[Dict]) -> List[Dict]:
    """
    Режим MANUAL
    Загружает пары из файла pairs.json
    """
    try:
        with open("pairs.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        
        pairs = []
        accounts_by_name = {acc["name"]: acc for acc in accounts}
        
        for pair in data.get("pairs", []):
            giver_name = pair.get("giver")
            receiver_name = pair.get("receiver")
            action = pair.get("action", "bless")
            
            if giver_name in accounts_by_name:
                giver = accounts_by_name[giver_name]
            else:
                print(f"⚠️ Аккаунт не найден: {giver_name}")
                continue
            
            if receiver_name in accounts_by_name:
                receiver = accounts_by_name[receiver_name]
            else:
                # Возможно это внешний username
                receiver = {
                    "name": receiver_name,
                    "discord_username": pair.get("discord_username", receiver_name)
                }
            
            pairs.append({
                "giver": giver,
                "receiver": receiver,
                "action": action
            })
        
        return pairs
        
    except FileNotFoundError:
        print("❌ Файл pairs.json не найден")
        print("   Создайте файл pairs.json со структурой:")
        print('   {"pairs": [{"giver": "Account 1", "receiver": "Account 2", "action": "bless"}]}')
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка в pairs.json: {e}")
        return []


# ============================================================================
# HELPERS
# ============================================================================

def get_random_delay(min_val: int, max_val: int) -> float:
    """Случайная задержка с небольшой вариацией"""
    base = random.uniform(min_val, max_val)
    variation = base * random.uniform(-0.2, 0.2)
    return max(1, base + variation)


async def maybe_random_pause(pause_config: RandomPauseConfig) -> None:
    """Случайная пауза для имитации человека"""
    if not pause_config.enabled:
        return
    
    if random.random() < pause_config.chance:
        pause = random.uniform(pause_config.min_seconds, pause_config.max_seconds)
        print(f"\n☕ Случайная пауза {pause:.0f} сек...")
        await asyncio.sleep(pause)


async def countdown_delay(seconds: float, message: str = "Ожидание") -> None:
    """Задержка с отображением обратного отсчёта"""
    remaining = int(seconds)
    print(f"\n⏳ {message}: {remaining} сек ({remaining/60:.1f} мин)")
    
    while remaining > 0 and not shutdown_handler.is_shutting_down:
        if remaining % 60 == 0 and remaining > 0:
            print(f"   ⏳ Осталось: {remaining} сек...")
        await asyncio.sleep(min(10, remaining))
        remaining -= 10


# ============================================================================
# ACTION EXECUTOR
# ============================================================================

async def execute_action(
    adspower: AdsPowerAPI,
    giver: Dict[str, Any],
    receiver: Dict[str, Any],
    action_type: str,
    channel_url: str,
    timing_config: TimingConfig,
    state_mgr: Optional[StateManager] = None
) -> bool:
    """Выполнить одно действие (bless или curse)"""
    
    giver_name = giver.get("name", "Unknown")
    receiver_name = receiver.get("name", "Unknown")
    receiver_discord = receiver.get("discord_username")
    adspower_id = giver.get("adspower_id", "")
    
    is_serial = adspower_id.isdigit() if adspower_id else False
    serial_number = int(adspower_id) if is_serial else None
    profile_id = None if is_serial else adspower_id
    profile_display = f"#{adspower_id}" if is_serial else adspower_id
    
    emoji = "✨" if action_type == "bless" else "💀"
    print(f"\n{'='*60}")
    print(f"{emoji} {action_type.upper()}: {giver_name} → {receiver_name}")
    print(f"   Профиль: {profile_display}")
    print(f"   Цель: @{receiver_discord}")
    print(f"{'='*60}")
    
    if not adspower_id or not receiver_discord:
        print(f"❌ Нет данных: adspower_id={adspower_id}, receiver={receiver_discord}")
        if state_mgr:
            state_mgr.record_action(giver_name, receiver_name, action_type, False)
        return False
    
    if shutdown_handler.is_shutting_down:
        return False
    
    profile_identifier = ProfileIdentifier(
        profile_id=profile_id,
        serial_number=serial_number,
        display_name=f"{giver_name} ({profile_display})"
    )
    
    # Запуск браузера
    print(f"\n🚀 Запуск браузера...")
    browser_info = await adspower.start_browser(
        profile_id=profile_id,
        serial_number=serial_number
    )
    
    if not browser_info:
        print(f"❌ Не удалось запустить браузер")
        if state_mgr:
            state_mgr.record_action(giver_name, receiver_name, action_type, False)
        return False
    
    shutdown_handler.register_profile(profile_identifier)
    
    print("⏳ Инициализация браузера...")
    await asyncio.sleep(5)
    
    success = False
    
    try:
        cdp_url = browser_info.get("cdp_url") or browser_info.get("ws_url")
        
        async with DiscordAutomation(cdp_url, timing=timing_config) as discord:
            if not discord.is_connected:
                print(f"❌ Не удалось подключиться к браузеру")
                if state_mgr:
                    state_mgr.record_action(giver_name, receiver_name, action_type, False)
                return False
            
            if shutdown_handler.is_shutting_down:
                return False
            
            print(f"\n🔗 Переход в канал Discord...")
            if not await discord.navigate_to_channel(channel_url):
                print(f"❌ Не удалось открыть канал")
                if state_mgr:
                    state_mgr.record_action(giver_name, receiver_name, action_type, False)
                return False
            
            print(f"\n⚡ Выполняю /{action_type} на @{receiver_discord}...")
            
            if action_type == "bless":
                success = await discord.execute_bless(receiver_discord)
            elif action_type == "curse":
                success = await discord.execute_curse(receiver_discord)
            
            if success:
                print(f"✅ {action_type.capitalize()} успешно!")
            else:
                print(f"❌ {action_type.capitalize()} не удался")
            
            await asyncio.sleep(3)
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        success = False
    
    finally:
        print(f"\n🛑 Закрываю браузер...")
        try:
            await adspower.stop_browser_async(
                profile_id=profile_id if profile_id else None,
                serial_number=serial_number
            )
            shutdown_handler.unregister_profile(profile_identifier)
        except Exception as e:
            print(f"⚠️ Ошибка закрытия: {e}")
    
    if state_mgr:
        state_mgr.record_action(giver_name, receiver_name, action_type, success)
    
    return success


# ============================================================================
# MAIN SESSION RUNNER
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
    max_actions: Optional[int] = None
) -> None:
    """Запуск сессии автоматизации"""
    
    accounts = account_mgr.get_config_value("accounts", [])
    modes_config = account_mgr.get_config_value("modes", {})
    
    # Генерируем пары в зависимости от режима
    print(f"\n📋 Режим: {mode.upper()}")
    
    if mode == "chain":
        chain_config = modes_config.get("chain", {})
        both_actions = chain_config.get("both_bless_and_curse", True)
        pairs = generate_chain_pairs(accounts, both_actions)
        print(f"   Паровозик: каждый → следующий")
        
    elif mode == "target":
        target_config = modes_config.get("target", {})
        target_user = target_config.get("target_username", "")
        if not target_user:
            print("❌ Не указан target_username в конфиге")
            return
        pairs = generate_target_pairs(accounts, target_user)
        print(f"   Все аккаунты → @{target_user}")
        
    elif mode == "smart":
        state_mgr.update_settings(
            daily_limit_per_account=limits.daily_limit_per_account,
            target_bless=limits.target_bless,
            target_curse=limits.target_curse
        )
        max_act = max_actions or limits.max_actions_per_session
        pairs = generate_smart_pairs(accounts, state_mgr, limits, max_act)
        print(f"   Умный режим: автовыбор кому нужны bless/curse")
        
    elif mode == "manual":
        pairs = load_manual_pairs(accounts)
        print(f"   Ручной режим: из pairs.json")
        
    else:
        print(f"❌ Неизвестный режим: {mode}")
        return
    
    if not pairs:
        print("\n✅ Нет действий для выполнения!")
        return
    
    # Применяем лимит на сессию
    max_act = max_actions or limits.max_actions_per_session
    if limits.enabled and len(pairs) > max_act:
        print(f"\n⚠️ Ограничено до {max_act} действий (из {len(pairs)})")
        pairs = pairs[:max_act]
    
    # Показываем план
    print(f"\n📝 Запланировано: {len(pairs)} действий")
    print("-"*50)
    for i, pair in enumerate(pairs, 1):
        g = pair["giver"]["name"]
        r = pair["receiver"].get("name", pair["receiver"].get("discord_username", "?"))
        a = pair["action"]
        emoji = "✨" if a == "bless" else "💀"
        print(f"   {i:2}. {emoji} {g} → {a} → {r}")
    print("-"*50)
    
    # Оценка времени
    avg_delay = (delays.between_commands_min + delays.between_commands_max) / 2
    estimated_time = len(pairs) * (avg_delay + 60) / 60  # +60 сек на само действие
    print(f"\n⏱️ Примерное время: {estimated_time:.0f} мин")
    
    # Выполняем
    completed = 0
    failed = 0
    current_giver = None
    
    for i, pair in enumerate(pairs):
        if shutdown_handler.is_shutting_down:
            print("\n⚠️ Прерывание...")
            break
        
        giver = pair["giver"]
        receiver = pair["receiver"]
        action_type = pair["action"]
        
        print(f"\n{'='*60}")
        print(f"📊 Прогресс: {i+1}/{len(pairs)}")
        print(f"{'='*60}")
        
        # Пауза при смене аккаунта
        if current_giver and current_giver != giver.get("name"):
            delay = get_random_delay(delays.between_accounts_min, delays.between_accounts_max)
            await countdown_delay(delay, "Смена аккаунта")
        
        current_giver = giver.get("name")
        
        # Выполняем действие
        success = await execute_action(
            adspower=adspower,
            giver=giver,
            receiver=receiver,
            action_type=action_type,
            channel_url=channel_url,
            timing_config=timing,
            state_mgr=state_mgr if mode == "smart" else None
        )
        
        if success:
            completed += 1
        else:
            failed += 1
        
        # Пауза между действиями
        if i < len(pairs) - 1 and not shutdown_handler.is_shutting_down:
            next_giver = pairs[i + 1]["giver"].get("name")
            
            if next_giver == current_giver:
                delay = get_random_delay(delays.between_commands_min, delays.between_commands_max)
                print(f"\n⏳ Пауза: {delay:.0f} сек...")
                await asyncio.sleep(delay)
            
            await maybe_random_pause(pauses)
    
    # Итог
    print(f"\n{'='*60}")
    print(f"📊 ИТОГИ СЕССИИ")
    print(f"{'='*60}")
    print(f"✅ Успешно: {completed}")
    print(f"❌ Ошибок: {failed}")
    if completed + failed > 0:
        print(f"📈 Успешность: {completed/(completed+failed)*100:.0f}%")


# ============================================================================
# MAIN
# ============================================================================

async def main_async(args) -> None:
    """Основная функция"""
    
    print("="*60)
    print("🤖 Discord RPA Automation")
    print("="*60 + "\n")
    
    account_mgr = None
    state_mgr = None
    adspower = None
    
    try:
        account_mgr = AccountManager("config.json")
        if not account_mgr.load_config():
            print("❌ Не удалось загрузить config.json")
            return
        
        state_mgr = StateManager("state.json")
        
        # Загрузка конфигов
        mode = args.mode or account_mgr.get_config_value("mode", "chain")
        delays = load_delay_config(account_mgr)
        limits = load_limits_config(account_mgr)
        pauses = load_pause_config(account_mgr)
        timing = load_timing_config(account_mgr)
        channel_url = account_mgr.get_config_value("discord_channel_url")
        
        if not channel_url:
            print("❌ discord_channel_url не настроен")
            return
        
        # Показываем настройки
        print(f"⚙️ Настройки:")
        print(f"   Режим: {mode}")
        print(f"   Задержки: {delays.between_commands_min}-{delays.between_commands_max}с между командами")
        print(f"   Лимиты: {'включены' if limits.enabled else 'выключены'}")
        print(f"   Случайные паузы: {'включены' if pauses.enabled else 'выключены'}")
        
        # Режим только статуса
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
    print("\n\n⚠️ Ctrl+C - завершаю...")
    shutdown_handler.is_shutting_down = True


def main():
    parser = argparse.ArgumentParser(
        description="Discord RPA Automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Режимы работы:
  chain   - Паровозик: каждый аккаунт кидает на следующего по кругу
  smart   - Умный: автоматически определяет кому нужны bless/curse
  target  - Все аккаунты кидают на одну цель (указать в конфиге)
  manual  - Ручной список пар из pairs.json

Примеры:
  python main.py                    # Режим из конфига
  python main.py -m chain           # Режим паровозик
  python main.py -m smart -l 10     # Умный режим, max 10 действий
  python main.py --status           # Только показать прогресс
        """
    )
    
    parser.add_argument("-m", "--mode", choices=["chain", "smart", "target", "manual"],
                        help="Режим работы (по умолчанию из конфига)")
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
