"""
Discord Automation Module
Handles Discord navigation and command execution using Patchright (anti-detect Playwright fork)
"""
import asyncio
import random
import time
from typing import Optional
from dataclasses import dataclass

try:
    from patchright.async_api import async_playwright, Page, Browser, BrowserContext
    PATCHRIGHT_AVAILABLE = True
except ImportError:
    PATCHRIGHT_AVAILABLE = False
    print("❌ Patchright not installed. Run: pip install patchright")
    print("   Then run: patchright install chrome")


@dataclass
class TimingConfig:
    """Configuration for human-like timing delays"""
    typing_delay_min: int = 50      # Min delay between keystrokes (ms)
    typing_delay_max: int = 150     # Max delay between keystrokes (ms)
    action_delay_min: int = 200     # Min delay between actions (ms)
    action_delay_max: int = 500     # Max delay between actions (ms)
    autocomplete_wait: float = 2.0  # Wait for autocomplete to appear (s)
    command_submit_wait: float = 3.0  # Wait after command submission (s)
    bot_response_timeout: float = 5.0  # Timeout for bot response verification (s)


class DiscordAutomation:
    """Automates Discord interactions using Patchright (stealth Playwright)"""
    
    def __init__(self, cdp_url: str, timing: Optional[TimingConfig] = None) -> None:
        """
        Initialize Discord automation
        
        Args:
            cdp_url: Chrome DevTools Protocol URL from AdsPower (e.g., http://127.0.0.1:9222)
            timing: Optional timing configuration for human-like behavior
            
        Raises:
            ValueError: If CDP URL is empty
        """
        if not cdp_url:
            raise ValueError("CDP URL cannot be empty")
        
        self.cdp_url = cdp_url
        self.timing = timing or TimingConfig()
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self._connected = False
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    def _ensure_connected(self) -> None:
        """
        Ensure browser is connected
        
        Raises:
            RuntimeError: If browser is not connected
        """
        if not self._connected or not self.page:
            raise RuntimeError("Browser not connected. Call connect() first.")
        
    async def connect(self) -> bool:
        """
        Connect to the browser via CDP with built-in stealth
        
        Returns:
            bool: True if connected successfully
        """
        if not PATCHRIGHT_AVAILABLE:
            print("❌ Patchright is not installed")
            return False
            
        try:
            self.playwright = await async_playwright().start()
            
            # Connect to existing AdsPower browser via CDP
            print(f"🔗 Connecting to browser via CDP: {self.cdp_url}")
            self.browser = await self.playwright.chromium.connect_over_cdp(self.cdp_url)
            
            # Get the first context and page, or create new ones
            contexts = self.browser.contexts
            if contexts:
                self.context = contexts[0]
                if self.context.pages:
                    self.page = self.context.pages[0]
                    print("  ✓ Using existing page")
                else:
                    self.page = await self.context.new_page()
                    print("  ✓ Created new page in existing context")
            else:
                self.context = await self.browser.new_context()
                self.page = await self.context.new_page()
                print("  ✓ Created new context and page")
            
            self._connected = True
            print("✅ Connected to browser via Patchright (stealth mode active)")
            print("🥷 Anti-detection: navigator.webdriver = undefined")
            return True
            
        except Exception as e:
            print(f"❌ Failed to connect to browser: {e}")
            import traceback
            traceback.print_exc()
            self._connected = False
            return False
    
    @property
    def is_connected(self) -> bool:
        """Check if browser is connected"""
        return self._connected and self.page is not None
    
    async def _human_type(self, text: str, delay_min: Optional[int] = None, delay_max: Optional[int] = None) -> None:
        """
        Type text with human-like delays using keyboard.type() for proper character input
        
        Args:
            text: Text to type
            delay_min: Minimum delay between keystrokes in milliseconds (uses config if None)
            delay_max: Maximum delay between keystrokes in milliseconds (uses config if None)
            
        Raises:
            RuntimeError: If not connected to browser
        """
        if not self.page:
            raise RuntimeError("Not connected to browser")
        
        delay_min = delay_min or self.timing.typing_delay_min
        delay_max = delay_max or self.timing.typing_delay_max
        
        for char in text:
            # Use keyboard.type() instead of keyboard.press() for proper character handling
            # keyboard.press() is for keys like "Enter", "Tab", "Control+a"
            # keyboard.type() is for typing actual text characters
            await self.page.keyboard.type(char)
            # Add human-like delay between keystrokes
            delay = random.randint(delay_min, delay_max) / 1000
            await asyncio.sleep(delay)
    
    async def _random_delay(self, min_ms: Optional[int] = None, max_ms: Optional[int] = None) -> None:
        """
        Add random delay to simulate human behavior
        
        Args:
            min_ms: Minimum delay in milliseconds (uses config if None)
            max_ms: Maximum delay in milliseconds (uses config if None)
        """
        min_ms = min_ms or self.timing.action_delay_min
        max_ms = max_ms or self.timing.action_delay_max
        delay = random.randint(min_ms, max_ms) / 1000
        await asyncio.sleep(delay)
    
    async def _clear_input(self) -> None:
        """Clear current input field using keyboard shortcuts (Ctrl+A, Backspace)"""
        if not self.page:
            return
        # Select all and delete
        await self.page.keyboard.press("Control+a")
        await asyncio.sleep(0.1)
        await self.page.keyboard.press("Backspace")
    
    async def _get_last_message_id(self) -> Optional[str]:
        """Get the ID of the last message in chat for comparison"""
        try:
            messages = await self.page.query_selector_all('[id^="chat-messages-"]')
            if messages:
                last_msg = messages[-1]
                return await last_msg.get_attribute("id")
            return None
        except Exception:
            return None
    
    async def _wait_for_bot_response(self, before_message_id: Optional[str], timeout: float = 5.0) -> bool:
        """
        Wait for a new message to appear after command submission
        
        Args:
            before_message_id: Message ID before command was sent
            timeout: Maximum wait time in seconds
            
        Returns:
            bool: True if new message appeared, False otherwise
        """
        try:
            start_time = time.monotonic()
            
            while time.monotonic() - start_time < timeout:
                current_id = await self._get_last_message_id()
                
                # If we have a new message (different ID or first message)
                if current_id and current_id != before_message_id:
                    print("  ✓ Bot response detected")
                    return True
                
                await asyncio.sleep(0.5)
            
            print("  ⚠️ No bot response detected (timeout)")
            return False
            
        except Exception as e:
            print(f"  ⚠️ Error checking bot response: {e}")
            return False
    
    async def _check_for_error_message(self) -> Optional[str]:
        """
        Check if there's an error message visible (e.g., cooldown, rate limit)
        
        Returns:
            Error message text if found, None otherwise
        """
        try:
            # Common error selectors in Discord
            error_selectors = [
                '[class*="error"]',
                '[class*="Error"]',
                '[class*="ephemeral"]',  # Ephemeral messages often contain errors
            ]
            
            for selector in error_selectors:
                elements = await self.page.query_selector_all(selector)
                for elem in elements:
                    text = await elem.text_content()
                    if text and any(word in text.lower() for word in ['cooldown', 'wait', 'error', 'failed', 'limit']):
                        return text.strip()
            
            return None
        except Exception:
            return None
    
    async def navigate_to_channel(self, channel_url: str, timeout: int = 45000) -> bool:
        """
        Navigate to Discord channel with robust loading checks
        
        Args:
            channel_url: Discord channel URL
            timeout: Maximum wait time in milliseconds
            
        Returns:
            bool: True if navigation successful
        """
        try:
            self._ensure_connected()
            print(f"🔗 Navigating to: {channel_url}")
            
            # Navigate to channel
            await self.page.goto(channel_url, wait_until="domcontentloaded", timeout=timeout)
            print("  ✓ Page loaded")
            
            # Wait for Discord app to initialize
            await self.page.wait_for_selector('[class*="app"]', timeout=timeout)
            print("  ✓ Discord app initialized")
            
            # Wait for channel content - message input or chat area
            await self.page.wait_for_selector(
                'div[role="textbox"], [class*="messagesWrapper"], [class*="chatContent"]',
                timeout=timeout
            )
            print("  ✓ Channel content loaded")
            
            # Wait for message input to be ready
            try:
                await self.page.wait_for_selector('div[role="textbox"]', state="visible", timeout=10000)
                print("  ✓ Message input ready")
            except Exception:
                print("  ⚠️ Message input not immediately visible, continuing...")
            
            # Additional wait for lazy-loaded elements
            await asyncio.sleep(3)
            
            print("✅ Discord channel fully loaded and ready")
            return True
            
        except Exception as e:
            print(f"❌ Error navigating to channel: {e}")
            return False
    
    async def execute_slash_command(self, command: str, target_user: Optional[str] = None, 
                                      timeout: int = 20000, verify_response: bool = True) -> bool:
        """
        Execute a Discord slash command with human-like behavior
        
        Args:
            command: Command name (e.g., 'bless', 'curse', 'stats', 'journey')
            target_user: Target user for commands that require it
            timeout: Maximum wait time in milliseconds
            verify_response: Whether to verify bot response after command
            
        Returns:
            bool: True if command executed successfully
        """
        try:
            self._ensure_connected()
            print(f"⚡ Executing command: /{command}" + (f" @{target_user}" if target_user else ""))
            
            # Get last message ID before sending command (for response verification)
            before_message_id = await self._get_last_message_id() if verify_response else None
            
            # Find and click message input
            print("  ⏳ Waiting for message input...")
            message_input = self.page.locator('div[role="textbox"]')
            await message_input.wait_for(state="visible", timeout=timeout)
            
            # Human-like delay before clicking
            await self._random_delay(200, 500)
            await message_input.click()
            await self._random_delay(500, 1000)
            print("  ✓ Message input focused")
            
            # Type the slash command with human-like delays
            print(f"  ⌨️ Typing /{command}...")
            await self._human_type(f"/{command}")
            
            # Wait for autocomplete
            await asyncio.sleep(self.timing.autocomplete_wait)
            
            # Check for autocomplete popup
            try:
                await self.page.wait_for_selector(
                    '[class*="autocomplete"], [class*="Autocomplete"], [role="listbox"]',
                    timeout=6000
                )
                print("  ✓ Command autocomplete appeared")
                await asyncio.sleep(0.5)
            except Exception:
                print("  ⚠️ No autocomplete detected, continuing...")
            
            # Press Enter to select command
            print("  ⏎ Selecting command...")
            await self.page.keyboard.press("Enter")
            await asyncio.sleep(1.5)
            
            # If target user specified
            if target_user:
                print(f"  👤 Entering target user: {target_user}")
                await self._random_delay(300, 600)
                
                # Type target user with human-like delays
                await self._human_type(target_user)
                print(f"  ⌨️ Typed: {target_user}")
                
                # Wait for user autocomplete
                await asyncio.sleep(self.timing.autocomplete_wait)
                
                try:
                    await self.page.wait_for_selector(
                        '[class*="autocomplete"], [class*="Autocomplete"], [role="listbox"]',
                        timeout=5000
                    )
                    print("  ✓ User autocomplete appeared")
                    await asyncio.sleep(0.5)
                except Exception:
                    print("  ⚠️ No user autocomplete, continuing...")
                
                # Select user
                print("  ⏎ Selecting user...")
                await self.page.keyboard.press("Enter")
                await asyncio.sleep(1)
            
            # Submit command
            print("  ⏎ Submitting command...")
            await self._random_delay(500, 800)
            await self.page.keyboard.press("Enter")
            
            # Wait for command to process
            await asyncio.sleep(self.timing.command_submit_wait)
            
            # Verify bot response if requested
            if verify_response:
                bot_responded = await self._wait_for_bot_response(
                    before_message_id, 
                    self.timing.bot_response_timeout
                )
                
                # Check for error messages
                error_msg = await self._check_for_error_message()
                if error_msg:
                    print(f"  ⚠️ Possible error detected: {error_msg[:100]}...")
                
                if not bot_responded:
                    print(f"⚠️ Command /{command} sent but no response detected")
                    return True  # Command was sent, just no verification
            
            print(f"✅ Command /{command} executed successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error executing command /{command}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def execute_bless(self, target_user: str) -> bool:
        """
        Execute /bless command on target user
        
        Args:
            target_user: Discord username to bless
            
        Returns:
            bool: True if successful, False otherwise
        """
        return await self.execute_slash_command("bless", target_user)
    
    async def execute_curse(self, target_user: str) -> bool:
        """
        Execute /curse command on target user
        
        Args:
            target_user: Discord username to curse
            
        Returns:
            bool: True if successful, False otherwise
        """
        return await self.execute_slash_command("curse", target_user)
    
    async def execute_stats(self) -> bool:
        """
        Execute /stats command
        
        Returns:
            bool: True if successful, False otherwise
        """
        return await self.execute_slash_command("stats")
    
    async def execute_journey(self) -> bool:
        """
        Execute /journey command
        
        Returns:
            bool: True if successful, False otherwise
        """
        return await self.execute_slash_command("journey")
    
    async def close(self) -> None:
        """
        Close the browser connection (disconnect only, AdsPower manages browser lifecycle)
        
        Note: This only disconnects from the browser, it doesn't close the browser itself.
        AdsPower is responsible for managing the browser lifecycle.
        """
        try:
            # Properly disconnect from browser before stopping playwright
            if self.browser:
                try:
                    # Disconnect from CDP, don't close (AdsPower manages browser)
                    await self.browser.close()
                except Exception as e:
                    print(f"⚠️ Error disconnecting browser: {e}")
            
            # Stop playwright
            if self.playwright:
                await self.playwright.stop()
            
            # Reset state
            self.browser = None
            self.context = None
            self.page = None
            self.playwright = None
            self._connected = False
            
            print("✅ Browser connection closed")
        except Exception as e:
            print(f"⚠️ Error closing browser: {e}")
            self._connected = False
