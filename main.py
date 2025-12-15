"""
Discord RPA Main Orchestrator
Automates Discord commands across multiple AdsPower accounts using Patchright
"""
import asyncio
import signal
import sys
from datetime import datetime
from typing import Union, Optional, List
from dataclasses import dataclass, field
from adspower_api import AdsPowerAPI
from discord_automation import DiscordAutomation, TimingConfig
from account_manager import AccountManager
from logger_config import setup_logger

# Setup logger
logger = setup_logger("RitualRPA", log_to_file=True)


@dataclass
class ProfileIdentifier:
    """Holds profile identification info (either profile_id or serial_number)"""
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
    """Handles graceful shutdown and cleanup of browser sessions"""
    
    adspower: Optional[AdsPowerAPI] = None
    active_profiles: List[ProfileIdentifier] = field(default_factory=list)
    is_shutting_down: bool = False
    
    def register_profile(self, profile: ProfileIdentifier) -> None:
        """Register an active browser profile for cleanup"""
        if profile and profile not in self.active_profiles:
            self.active_profiles.append(profile)
    
    def unregister_profile(self, profile: ProfileIdentifier) -> None:
        """Remove a profile from cleanup list after it's been closed"""
        if profile in self.active_profiles:
            self.active_profiles.remove(profile)
    
    async def cleanup(self) -> None:
        """Stop all active browser profiles"""
        if self.is_shutting_down:
            return
        
        self.is_shutting_down = True
        
        if not self.adspower or not self.active_profiles:
            return
        
        print("\n🛑 Shutting down - closing all browser sessions...")
        logger.info("Graceful shutdown initiated")
        
        for profile in self.active_profiles.copy():
            try:
                display = profile.display_name or profile.profile_id or f"#{profile.serial_number}"
                print(f"  ⏳ Stopping browser: {display}")
                await self.adspower.stop_browser_async(
                    profile_id=profile.profile_id,
                    serial_number=profile.serial_number
                )
                self.active_profiles.remove(profile)
            except Exception as e:
                print(f"  ⚠️ Error stopping {display}: {e}")
        
        # Close aiohttp session
        await self.adspower.close()
        
        print("✅ All browser sessions closed")
        logger.info("Graceful shutdown completed")


# Global shutdown handler
shutdown_handler = ShutdownHandler()


def load_timing_config(account_mgr: AccountManager) -> TimingConfig:
    """
    Load timing configuration from config file
    
    Args:
        account_mgr: Account manager instance
        
    Returns:
        TimingConfig with values from config or defaults
    """
    timing_dict = account_mgr.get_config_value("timing", {})
    
    return TimingConfig(
        typing_delay_min=timing_dict.get("typing_delay_min", 50),
        typing_delay_max=timing_dict.get("typing_delay_max", 150),
        action_delay_min=timing_dict.get("action_delay_min", 200),
        action_delay_max=timing_dict.get("action_delay_max", 500),
        autocomplete_wait=timing_dict.get("autocomplete_wait", 2.0),
        command_submit_wait=timing_dict.get("command_submit_wait", 3.0),
        bot_response_timeout=timing_dict.get("bot_response_timeout", 5.0)
    )


async def process_account(adspower: AdsPowerAPI, account_mgr: AccountManager, 
                          pair: dict, channel_url: str, delay_between_commands: Union[int, float],
                          timing_config: Optional[TimingConfig] = None) -> None:
    """Process a single account - execute bless and curse commands"""
    
    current_account = pair["current"]
    target_account = pair["target"]
    account_name = current_account.get("name", "Unknown")
    adspower_id = current_account.get("adspower_id", "")
    target_user = target_account.get("discord_username")
    
    # Determine if adspower_id is a serial number (numeric) or profile ID (alphanumeric)
    is_serial = adspower_id.isdigit() if adspower_id else False
    serial_number = int(adspower_id) if is_serial else None
    profile_id = None if is_serial else adspower_id
    
    # Display identifier for logging
    profile_display = f"#{adspower_id}" if is_serial else adspower_id
    
    print("="*60)
    print(f"🔄 Processing Account {pair['index']}/{pair['total']}: {account_name}")
    print(f"   Profile: {profile_display}")
    print(f"   Target: {target_user}")
    print("="*60)
    
    logger.info(f"Processing account: {account_name} (Profile: {profile_display}, Target: {target_user})")
    
    # Check if we have identifier
    if not adspower_id:
        msg = f"Skipping {account_name}: No AdsPower ID configured"
        print(f"⚠️ {msg}")
        logger.warning(msg)
        account_mgr.log_execution(account_name, "skip", False, "No AdsPower ID")
        return
    
    if not target_user:
        msg = f"Skipping {account_name}: No target user configured"
        print(f"⚠️ {msg}")
        logger.warning(msg)
        account_mgr.log_execution(account_name, "skip", False, "No target user")
        return
    
    # Check if shutdown requested
    if shutdown_handler.is_shutting_down:
        print(f"⚠️ Shutdown requested, skipping {account_name}")
        return
    
    # Create profile identifier for tracking
    profile_identifier = ProfileIdentifier(
        profile_id=profile_id,
        serial_number=serial_number,
        display_name=f"{account_name} ({profile_display})"
    )
    
    # Start browser
    print(f"\n🚀 Starting browser for {account_name}...")
    browser_info = await adspower.start_browser(
        profile_id=profile_id,
        serial_number=serial_number
    )
    
    if not browser_info:
        msg = f"Failed to start browser for {account_name}"
        print(f"❌ {msg}")
        logger.error(msg)
        account_mgr.log_execution(account_name, "browser_start", False, "Failed to start")
        return
    
    # Register profile for cleanup
    shutdown_handler.register_profile(profile_identifier)
    
    # Wait for browser to fully initialize
    print("⏳ Waiting for browser to initialize...")
    await asyncio.sleep(5)
    
    try:
        # Connect to browser via CDP using Patchright
        cdp_url = browser_info.get("cdp_url") or browser_info.get("ws_url")
        
        async with DiscordAutomation(cdp_url, timing=timing_config) as discord:
            if not discord.is_connected:
                print(f"❌ Failed to connect to browser for {account_name}")
                account_mgr.log_execution(account_name, "browser_connect", False, "Connection failed")
                return
            
            # Check shutdown again after connection
            if shutdown_handler.is_shutting_down:
                print(f"⚠️ Shutdown requested, stopping {account_name}")
                return
            
            # Navigate to Discord channel
            print(f"\n🔗 Navigating to Discord channel...")
            if not await discord.navigate_to_channel(channel_url):
                print(f"❌ Failed to navigate to Discord for {account_name}")
                account_mgr.log_execution(account_name, "navigate", False, "Navigation failed")
                return
            
            # Execute /bless command
            print(f"\n✨ Executing /bless on {target_user}...")
            bless_success = await discord.execute_bless(target_user)
            account_mgr.log_execution(
                account_name, 
                "bless", 
                bless_success,
                f"Target: {target_user}"
            )
            
            if bless_success:
                print(f"✅ Blessed {target_user}")
            else:
                print(f"❌ Failed to bless {target_user}")
            
            # Wait between commands
            print(f"\n⏳ Waiting {delay_between_commands}s before next command...")
            await asyncio.sleep(delay_between_commands)
            
            # Check shutdown before next command
            if shutdown_handler.is_shutting_down:
                print(f"⚠️ Shutdown requested, stopping after bless")
                return
            
            # Execute /curse command
            print(f"\n💀 Executing /curse on {target_user}...")
            curse_success = await discord.execute_curse(target_user)
            account_mgr.log_execution(
                account_name,
                "curse",
                curse_success,
                f"Target: {target_user}"
            )
            
            if curse_success:
                print(f"✅ Cursed {target_user}")
            else:
                print(f"❌ Failed to curse {target_user}")
            
            # Wait for final command to process
            print("\n⏳ Waiting for command to complete...")
            await asyncio.sleep(2)
            
            print(f"\n✅ Completed processing for {account_name}")
        
    except Exception as e:
        msg = f"Error processing {account_name}: {e}"
        print(f"\n❌ {msg}")
        logger.error(msg, exc_info=True)
        account_mgr.log_execution(account_name, "error", False, str(e))
    
    finally:
        # Always stop browser
        print(f"\n🛑 Stopping browser for {account_name}...")
        try:
            await adspower.stop_browser_async(
                profile_id=profile_id if profile_id else None,
                serial_number=serial_number
            )
            shutdown_handler.unregister_profile(profile_identifier)
        except Exception as e:
            print(f"⚠️ Error stopping browser: {e}")


async def main() -> None:
    """
    Main async execution function
    
    Orchestrates the entire RPA automation process:
    1. Loads configuration
    2. Validates AdsPower connection
    3. Processes each account in sequence
    4. Saves execution logs
    """
    print("="*60)
    print("🤖 Discord RPA Automation (Patchright)")
    print("="*60 + "\n")
    
    logger.info("="*60)
    logger.info("Starting Discord RPA Automation")
    logger.info("="*60)
    
    account_mgr = None
    adspower = None
    
    try:
        # Initialize account manager
        account_mgr = AccountManager("config.json")
        if not account_mgr.load_config():
            print("❌ Failed to load configuration. Please check config.json")
            logger.error("Failed to load configuration from config.json")
            return
        
        # Initialize AdsPower API
        api_url = account_mgr.get_config_value("adspower_api_url", "http://localhost:50325")
        adspower = AdsPowerAPI(api_url)
        
        # Register adspower for graceful shutdown
        shutdown_handler.adspower = adspower
        
        # Check AdsPower connection
        print("🔍 Checking AdsPower connection...")
        logger.info(f"Checking AdsPower connection at {api_url}")
        if not adspower.check_connection():
            print("❌ Cannot connect to AdsPower. Please ensure it's running.")
            logger.error(f"Cannot connect to AdsPower at {api_url}")
            return
        print("✅ AdsPower is running\n")
        logger.info("AdsPower connection successful")
        
        # Get configuration
        channel_url = account_mgr.get_config_value("discord_channel_url")
        delay_between_accounts = account_mgr.get_config_value("delay_between_accounts", 8)
        delay_between_commands = account_mgr.get_config_value("delay_between_commands", 5)
        
        # Load timing configuration
        timing_config = load_timing_config(account_mgr)
        
        if not channel_url:
            print("❌ Discord channel URL not configured")
            return
        
        # Get account pairs
        account_pairs = account_mgr.get_account_pairs()
        print(f"📋 Processing {len(account_pairs)} accounts in chain mode\n")
        
        # Process each account
        for pair in account_pairs:
            # Check if shutdown requested
            if shutdown_handler.is_shutting_down:
                print("\n⚠️ Shutdown requested, stopping processing...")
                break
            
            await process_account(
                adspower, account_mgr, pair,
                channel_url, delay_between_commands, timing_config
            )
            
            # Wait before next account
            if pair["index"] < pair["total"] and not shutdown_handler.is_shutting_down:
                print(f"\n⏳ Waiting {delay_between_accounts}s before next account...\n")
                await asyncio.sleep(delay_between_accounts)
        
        if not shutdown_handler.is_shutting_down:
            print("🎉 RPA automation completed!")
            logger.info("RPA automation completed successfully")
        else:
            print("⚠️ RPA automation interrupted")
            logger.warning("RPA automation was interrupted by user")
        
    except Exception as e:
        print(f"\n❌ Unexpected error in main: {e}")
        logger.critical(f"Unexpected error in main: {e}", exc_info=True)
    
    finally:
        # Cleanup any remaining browsers
        await shutdown_handler.cleanup()
        
        # Always print summary and save log, even if errors occurred
        try:
            if account_mgr:
                account_mgr.print_summary()
                # Save execution log with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                log_filename = f"execution_log_{timestamp}.json"
                account_mgr.save_log(log_filename)
                logger.info(f"Execution log saved to {log_filename}")
        except Exception as e:
            print(f"⚠️ Error saving final logs: {e}")
            logger.error(f"Error saving final logs: {e}", exc_info=True)
        
        # Close aiohttp session if still open
        if adspower:
            try:
                await adspower.close()
            except Exception:
                pass


def handle_sigint(signum, frame):
    """Handle Ctrl+C signal"""
    print("\n\n⚠️ Ctrl+C received - initiating graceful shutdown...")
    shutdown_handler.is_shutting_down = True


if __name__ == "__main__":
    # Setup signal handler for graceful shutdown
    signal.signal(signal.SIGINT, handle_sigint)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ Automation interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
