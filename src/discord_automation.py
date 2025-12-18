"""
Discord Automation Module
Handles Discord navigation and command execution using Patchright (anti-detect Playwright fork)
"""
import asyncio
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Tuple

try:
    from patchright.async_api import async_playwright, Page, Browser, BrowserContext, Locator
    PATCHRIGHT_AVAILABLE = True
except ImportError:
    PATCHRIGHT_AVAILABLE = False
    Page = None
    Browser = None
    BrowserContext = None
    Locator = None


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class TimingConfig:
    """Configuration for human-like timing delays"""
    typing_delay_min: int = 50
    typing_delay_max: int = 150
    action_delay_min: int = 200
    action_delay_max: int = 500
    autocomplete_wait: float = 2.0
    command_submit_wait: float = 3.0
    bot_response_timeout: float = 5.0


@dataclass
class DiscordSelectors:
    """CSS selectors for Discord UI elements"""
    
    # App container selectors
    app: List[str] = field(default_factory=lambda: [
        '[class*="app-"]',
        '[class*="layers-"]',
        '[data-list-id]',
        '#app-mount',
    ])
    
    # Channel content selectors
    channel_content: List[str] = field(default_factory=lambda: [
        'div[role="textbox"]',
        '[data-list-id="chat-messages"]',
        'main[class*="chat-"]',
        'div[class*="chat-"]',
        'ol[data-list-id="chat-messages"]',
        '[class*="scrollerInner-"]',
        'form[class*="form-"]',
    ])
    
    # Message input selectors
    message_input: List[str] = field(default_factory=lambda: [
        'div[role="textbox"]',
        '[contenteditable="true"][data-slate-editor="true"]',
        '[contenteditable="true"]',
        'div[class*="textArea-"] div[role="textbox"]',
    ])
    
    # Autocomplete selectors
    autocomplete: List[str] = field(default_factory=lambda: [
        '[class*="autocomplete"]',
        '[class*="Autocomplete"]',
        '[role="listbox"]',
    ])
    
    # Login indicators
    logged_in: List[str] = field(default_factory=lambda: [
        '[class*="avatar-"]',
        '[aria-label="User Settings"]',
        '[class*="panels-"]',
        'button[class*="button-"]',
    ])
    
    # Error selectors
    errors: List[str] = field(default_factory=lambda: [
        '[class*="error"]',
        '[class*="Error"]',
        '[class*="ephemeral"]',
    ])
    
    # Access issue selectors
    access_issues: List[str] = field(default_factory=lambda: [
        'text="You don\'t have access to this channel"',
        'text="This channel is read-only"',
        '[class*="notice-"]',
        '[class*="error"]',
    ])
    
    # Chat messages
    messages: str = '[id^="chat-messages-"]'


# ============================================================================
# MAIN CLASS
# ============================================================================

class DiscordAutomation:
    """Automates Discord interactions using Patchright (stealth Playwright)"""
    
    def __init__(
        self, 
        cdp_url: str, 
        timing: Optional[TimingConfig] = None,
        selectors: Optional[DiscordSelectors] = None,
        logger: Optional[logging.Logger] = None
    ) -> None:
        """
        Initialize Discord automation.
        
        Args:
            cdp_url: Chrome DevTools Protocol URL from AdsPower
            timing: Timing configuration for human-like behavior
            selectors: CSS selectors configuration
            logger: Optional logger instance
        """
        if not cdp_url:
            raise ValueError("CDP URL cannot be empty")
        
        self.cdp_url = cdp_url
        self.timing = timing or TimingConfig()
        self.selectors = selectors or DiscordSelectors()
        self.logger = logger or logging.getLogger(__name__)
        
        # Browser state
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self._connected = False
    
    # ========================================================================
    # CONTEXT MANAGER
    # ========================================================================
    
    async def __aenter__(self) -> "DiscordAutomation":
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
    
    # ========================================================================
    # CONNECTION
    # ========================================================================
    
    @property
    def is_connected(self) -> bool:
        """Check if browser is connected."""
        return self._connected and self.page is not None
    
    def _ensure_connected(self) -> None:
        """Ensure browser is connected, raise if not."""
        if not self._connected or not self.page:
            raise RuntimeError("Browser not connected. Call connect() first.")
    
    async def connect(self) -> bool:
        """Connect to the browser via CDP with built-in stealth."""
        if not PATCHRIGHT_AVAILABLE:
            self._log("❌ Patchright is not installed")
            return False
        
        try:
            self.playwright = await async_playwright().start()
            
            self._log(f"🔗 Connecting to browser via CDP: {self.cdp_url}")
            self.browser = await self.playwright.chromium.connect_over_cdp(self.cdp_url)
            
            # Get existing context/page or create new
            contexts = self.browser.contexts
            if contexts:
                self.context = contexts[0]
                if self.context.pages:
                    self.page = self.context.pages[0]
                    self._log("  ✓ Using existing page")
                else:
                    self.page = await self.context.new_page()
                    self._log("  ✓ Created new page in existing context")
            else:
                self.context = await self.browser.new_context()
                self.page = await self.context.new_page()
                self._log("  ✓ Created new context and page")
            
            self._connected = True
            self._log("✅ Connected to browser via Patchright (stealth mode active)")
            self._log("🥷 Anti-detection: navigator.webdriver = undefined")
            return True
            
        except Exception as e:
            self._log(f"❌ Failed to connect to browser: {e}", level="error")
            self._connected = False
            return False
    
    async def close(self, timeout: float = 10.0) -> None:
        """Close the browser connection (AdsPower manages browser lifecycle)."""
        try:
            if self.browser:
                try:
                    # Добавляем таймаут на закрытие браузера
                    await asyncio.wait_for(
                        self.browser.close(),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    self._log(f"⚠️ Browser close timed out after {timeout}s, forcing disconnect", level="warning")
                except Exception as e:
                    self._log(f"⚠️ Error disconnecting browser: {e}", level="warning")
            
            if self.playwright:
                try:
                    await asyncio.wait_for(
                        self.playwright.stop(),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    self._log("⚠️ Playwright stop timed out", level="warning")
                except Exception:
                    pass
            
            self._reset_state()
            self._log("✅ Browser connection closed")
            
        except Exception as e:
            self._log(f"⚠️ Error closing browser: {e}", level="warning")
            self._reset_state()
    
    def _reset_state(self) -> None:
        """Reset internal state."""
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None
        self._connected = False
    
    # ========================================================================
    # SELECTOR HELPERS
    # ========================================================================
    
    async def _find_element(
        self, 
        selectors: List[str], 
        timeout: int = 8000,
        state: str = "visible"
    ) -> Tuple[Optional[Locator], Optional[str]]:
        """
        Try to find an element using multiple selectors.
        
        Args:
            selectors: List of CSS selectors to try
            timeout: Timeout per selector in milliseconds
            state: Element state to wait for
            
        Returns:
            Tuple of (locator, matched_selector) or (None, None)
        """
        for selector in selectors:
            try:
                locator = self.page.locator(selector).first
                await locator.wait_for(state=state, timeout=timeout)
                return locator, selector
            except Exception:
                continue
        return None, None
    
    async def _wait_for_any_selector(
        self, 
        selectors: List[str], 
        timeout: int = 8000
    ) -> Optional[str]:
        """
        Wait for any of the given selectors to appear.
        
        Args:
            selectors: List of CSS selectors to try
            timeout: Timeout per selector in milliseconds
            
        Returns:
            Matched selector or None
        """
        for selector in selectors:
            try:
                await self.page.wait_for_selector(selector, timeout=timeout)
                return selector
            except Exception:
                continue
        return None
    
    async def _query_any_selector(self, selectors: List[str]) -> Tuple[Optional[any], Optional[str]]:
        """
        Query for any of the given selectors.
        
        Returns:
            Tuple of (element, matched_selector) or (None, None)
        """
        for selector in selectors:
            try:
                elem = await self.page.query_selector(selector)
                if elem:
                    return elem, selector
            except Exception:
                continue
        return None, None
    
    # ========================================================================
    # HUMAN-LIKE BEHAVIOR
    # ========================================================================
    
    async def _human_type(self, text: str) -> None:
        """Type text with human-like delays."""
        if not self.page:
            raise RuntimeError("Not connected to browser")
        
        for char in text:
            await self.page.keyboard.type(char)
            delay = random.randint(
                self.timing.typing_delay_min, 
                self.timing.typing_delay_max
            ) / 1000
            await asyncio.sleep(delay)
    
    async def _random_delay(self, min_ms: Optional[int] = None, max_ms: Optional[int] = None) -> None:
        """Add random delay to simulate human behavior."""
        min_ms = min_ms or self.timing.action_delay_min
        max_ms = max_ms or self.timing.action_delay_max
        delay = random.randint(min_ms, max_ms) / 1000
        await asyncio.sleep(delay)
    
    async def _clear_input(self) -> None:
        """Clear current input field using Ctrl+A, Backspace."""
        if self.page:
            await self.page.keyboard.press("Control+a")
            await asyncio.sleep(0.1)
            await self.page.keyboard.press("Backspace")
    
    # ========================================================================
    # DISCORD STATE CHECKS
    # ========================================================================
    
    async def verify_discord_login(self) -> bool:
        """Verify that Discord is logged in."""
        try:
            self._ensure_connected()
            
            current_url = self.page.url
            if "/login" in current_url or "/register" in current_url:
                self._log("  ❌ Not logged in - on login/register page")
                return False
            
            elem, selector = await self._query_any_selector(self.selectors.logged_in)
            if elem:
                self._log(f"  ✓ Discord logged in (found: {selector})")
                return True
            
            # Если не можем проверить, проверяем наличие элементов, указывающих на неавторизованность
            self._log("  ⚠️ Could not verify Discord login state")
            
            # Дополнительная проверка: ищем элементы, указывающие на страницу входа
            try:
                login_elements = await self.page.query_selector_all('input[type="email"], input[name="email"]')
                if login_elements:
                    self._log("  ❌ Found login form elements - likely not logged in")
                    return False
            except Exception:
                pass
            
            # Если URL не содержит /login и нет элементов входа, но и нет подтверждения авторизации,
            # считаем что авторизован (может быть медленная загрузка)
            return True
            
        except Exception as e:
            self._log(f"  ❌ Error verifying login: {e}", level="error")
            return False
    
    async def _check_channel_access(self) -> Optional[str]:
        """Check for Discord access issues."""
        try:
            for selector in self.selectors.access_issues:
                try:
                    elem = await self.page.query_selector(selector)
                    if elem:
                        text = await elem.text_content()
                        if text:
                            return text.strip()[:100]
                except Exception:
                    continue
            return None
        except Exception:
            return None
    
    async def _check_for_error_message(self) -> Optional[str]:
        """Check for error messages (cooldown, rate limit, etc.)."""
        try:
            error_keywords = ['cooldown', 'wait', 'error', 'failed', 'limit']
            
            for selector in self.selectors.errors:
                elements = await self.page.query_selector_all(selector)
                for elem in elements:
                    text = await elem.text_content()
                    if text and any(word in text.lower() for word in error_keywords):
                        return text.strip()
            return None
        except Exception:
            return None
    
    async def _get_last_message_id(self) -> Optional[str]:
        """Get the ID of the last message in chat."""
        try:
            messages = await self.page.query_selector_all(self.selectors.messages)
            if messages:
                return await messages[-1].get_attribute("id")
            return None
        except Exception:
            return None
    
    async def _wait_for_bot_response(self, before_message_id: Optional[str], timeout: float = 5.0) -> bool:
        """Wait for a new message to appear after command submission."""
        try:
            start_time = time.monotonic()
            
            while time.monotonic() - start_time < timeout:
                current_id = await self._get_last_message_id()
                if current_id and current_id != before_message_id:
                    self._log("  ✓ Bot response detected")
                    return True
                await asyncio.sleep(0.5)
            
            self._log("  ⚠️ No bot response detected (timeout)")
            return False
            
        except Exception as e:
            self._log(f"  ⚠️ Error checking bot response: {e}", level="warning")
            return False
    
    async def is_direct_message(self) -> bool:
        """
        Check if current channel is a direct message (DM).
        
        Returns:
            True if in DM, False if in server channel
        """
        try:
            self._ensure_connected()
            current_url = self.page.url
            
            # Discord DM URLs contain /@me/
            # Example: https://discord.com/channels/@me/123456789
            # Server channels: https://discord.com/channels/SERVER_ID/CHANNEL_ID
            is_dm = "/@me/" in current_url
            
            if is_dm:
                self._log("  ✓ Current channel is a Direct Message")
            else:
                self._log("  ℹ️ Current channel is a server channel")
            
            return is_dm
            
        except Exception as e:
            self._log(f"  ⚠️ Error checking channel type: {e}", level="warning")
            return False
    
    # ========================================================================
    # DEBUGGING
    # ========================================================================
    
    async def capture_screenshot(self, prefix: str = "debug") -> Optional[str]:
        """Capture a screenshot for debugging."""
        try:
            if not self.page:
                return None
            
            os.makedirs("screenshots", exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"screenshots/{prefix}_{timestamp}.png"
            
            await self.page.screenshot(path=filename, full_page=False)
            self._log(f"  📸 Debug screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            self._log(f"  ⚠️ Could not save debug screenshot: {e}", level="warning")
            return None
    
    def _log(self, message: str, level: str = "info") -> None:
        """Log message using logger and print."""
        print(message)
        log_method = getattr(self.logger, level, self.logger.info)
        log_method(message)
    
    # ========================================================================
    # NAVIGATION
    # ========================================================================
    
    async def navigate_to_channel(self, channel_url: str, timeout: int = 45000, warn_if_not_dm: bool = True) -> bool:
        """
        Navigate to Discord channel with robust loading checks.
        
        Args:
            channel_url: Discord channel URL
            timeout: Maximum wait time in milliseconds
            warn_if_not_dm: If True, warn if navigating to a server channel instead of DM
            
        Returns:
            True if navigation successful
        """
        try:
            self._ensure_connected()
            self._log(f"🔗 Navigating to: {channel_url}")
            
            # Check if URL is a DM
            if warn_if_not_dm and "/@me/" not in channel_url:
                self._log("⚠️ Внимание: это канал сервера, а не личные сообщения!")
                self._log("  💡 Для работы в личных сообщениях используйте URL вида: https://discord.com/channels/@me/CHANNEL_ID")
            
            # Navigate
            await self.page.goto(channel_url, wait_until="domcontentloaded", timeout=timeout)
            self._log("  ✓ Page loaded")
            
            # Check URL
            current_url = self.page.url
            self._log(f"  ℹ️ Current URL: {current_url}")
            
            if "/login" in current_url:
                self._log("  ❌ Redirected to login page - account not logged in!")
                return False
            
            # Wait for Discord app
            matched = await self._wait_for_any_selector(self.selectors.app, timeout=10000)
            if matched:
                self._log(f"  ✓ Discord app initialized (found: {matched})")
            else:
                self._log("  ⚠️ Discord app container not found, checking page state...")
                page_title = await self.page.title()
                self._log(f"  ℹ️ Page title: {page_title}")
            
            # Wait for channel content
            matched = await self._wait_for_any_selector(self.selectors.channel_content, timeout=8000)
            if matched:
                self._log(f"  ✓ Channel content loaded (found: {matched})")
            else:
                # Fallback: wait for any interactive element
                self._log("  ⚠️ Standard content selectors not found, trying alternative...")
                try:
                    await self.page.wait_for_selector(
                        'form, [contenteditable="true"], [role="textbox"]',
                        timeout=10000
                    )
                    self._log("  ✓ Interactive element found")
                except Exception:
                    self._log("  ❌ No interactive content found")
            
            # Wait for message input
            input_ready = await self._wait_for_message_input(timeout=15000)
            
            await asyncio.sleep(2)  # Wait for lazy-loaded elements
            
            # Final check
            textbox = await self.page.query_selector('div[role="textbox"]')
            if textbox:
                self._log("✅ Discord channel fully loaded and ready")
                return True
            
            # Check access issues
            error = await self._check_channel_access()
            if error:
                self._log(f"  ❌ Channel access issue: {error}")
                return False
            
            self._log("⚠️ Channel loaded but message input not found - may have limited access")
            return input_ready
            
        except Exception as e:
            self._log(f"❌ Error navigating to channel: {e}", level="error")
            await self.capture_screenshot("navigation_error")
            return False
    
    async def _wait_for_message_input(self, timeout: int = 15000) -> bool:
        """Wait for message input to be ready."""
        try:
            await self.page.wait_for_selector('div[role="textbox"]', state="visible", timeout=timeout)
            self._log("  ✓ Message input ready")
            return True
        except Exception:
            try:
                await self.page.wait_for_selector('[contenteditable="true"]', state="visible", timeout=5000)
                self._log("  ✓ Contenteditable input ready")
                return True
            except Exception:
                self._log("  ⚠️ Message input not immediately visible, continuing...")
                return False
    
    # ========================================================================
    # COMMAND EXECUTION
    # ========================================================================
    
    async def execute_slash_command(
        self, 
        command: str, 
        target_user: Optional[str] = None,
        timeout: int = 20000, 
        verify_response: bool = True,
        dm_only: bool = True
    ) -> bool:
        """
        Execute a Discord slash command with human-like behavior.
        
        Args:
            command: Command name (e.g., 'bless', 'curse', 'stats')
            target_user: Target user for commands that require it
            timeout: Maximum wait time in milliseconds
            verify_response: Whether to verify bot response
            dm_only: If True, only execute command in Direct Messages (default: True)
            
        Returns:
            True if command executed successfully
        """
        try:
            self._ensure_connected()
            target_str = f" @{target_user}" if target_user else ""
            self._log(f"⚡ Executing command: /{command}{target_str}")
            
            # Check if we're in a DM (if dm_only is enabled)
            if dm_only:
                if not await self.is_direct_message():
                    self._log(f"❌ Команда /{command} не выполнена: бот работает только в личных сообщениях")
                    self._log("  💡 Откройте личные сообщения с ботом для выполнения команд")
                    return False
            
            # Get last message ID for verification
            before_message_id = await self._get_last_message_id() if verify_response else None
            
            # Find message input
            message_input = await self._find_message_input(timeout)
            if not message_input:
                return False
            
            # Focus input
            await self._random_delay(200, 500)
            await message_input.click()
            await self._random_delay(500, 1000)
            self._log("  ✓ Message input focused")
            
            # Type command
            self._log(f"  ⌨️ Typing /{command}...")
            await self._human_type(f"/{command}")
            
            # Wait for autocomplete
            await asyncio.sleep(self.timing.autocomplete_wait)
            await self._wait_for_autocomplete()
            
            # Select command
            self._log("  ⏎ Selecting command...")
            await self.page.keyboard.press("Enter")
            await asyncio.sleep(1.5)
            
            # Enter target user if specified
            if target_user:
                await self._enter_target_user(target_user)
            
            # Submit command
            self._log("  ⏎ Submitting command...")
            await self._random_delay(500, 800)
            await self.page.keyboard.press("Enter")
            
            await asyncio.sleep(self.timing.command_submit_wait)
            
            # Verify response
            if verify_response:
                await self._verify_command_response(command, before_message_id)
            
            self._log(f"✅ Command /{command} executed successfully")
            return True
            
        except Exception as e:
            self._log(f"❌ Error executing command /{command}: {e}", level="error")
            return False
    
    async def _find_message_input(self, timeout: int) -> Optional[Locator]:
        """Find the message input field."""
        self._log("  ⏳ Waiting for message input...")
        
        locator, selector = await self._find_element(
            self.selectors.message_input, 
            timeout=min(timeout, 5000)
        )
        
        if locator:
            self._log(f"  ✓ Found message input: {selector}")
            return locator
        
        self._log("  ❌ Could not find message input field")
        await self.capture_screenshot("no_input_field")
        return None
    
    async def _wait_for_autocomplete(self) -> bool:
        """Wait for autocomplete popup to appear."""
        try:
            selector = ",".join(self.selectors.autocomplete)
            await self.page.wait_for_selector(selector, timeout=6000)
            self._log("  ✓ Command autocomplete appeared")
            await asyncio.sleep(0.5)
            return True
        except Exception:
            self._log("  ⚠️ No autocomplete detected, continuing...")
            return False
    
    async def _enter_target_user(self, target_user: str) -> None:
        """Enter target user for the command."""
        self._log(f"  👤 Entering target user: {target_user}")
        await self._random_delay(300, 600)
        
        await self._human_type(target_user)
        self._log(f"  ⌨️ Typed: {target_user}")
        
        await asyncio.sleep(self.timing.autocomplete_wait)
        await self._wait_for_autocomplete()
        
        self._log("  ⏎ Selecting user...")
        await self.page.keyboard.press("Enter")
        await asyncio.sleep(1)
    
    async def _verify_command_response(self, command: str, before_message_id: Optional[str]) -> None:
        """Verify bot response after command submission."""
        bot_responded = await self._wait_for_bot_response(
            before_message_id,
            self.timing.bot_response_timeout
        )
        
        error_msg = await self._check_for_error_message()
        if error_msg:
            self._log(f"  ⚠️ Possible error detected: {error_msg[:100]}...")
        
        if not bot_responded:
            self._log(f"⚠️ Command /{command} sent but no response detected")
    
    # ========================================================================
    # CONVENIENCE METHODS
    # ========================================================================
    
    async def execute_bless(self, target_user: str, dm_only: bool = True) -> bool:
        """
        Execute /bless command on target user.
        
        Args:
            target_user: Target user for the blessing
            dm_only: If True, only execute in Direct Messages (default: True)
        """
        return await self.execute_slash_command("bless", target_user, dm_only=dm_only)
    
    async def execute_curse(self, target_user: str, dm_only: bool = True) -> bool:
        """
        Execute /curse command on target user.
        
        Args:
            target_user: Target user for the curse
            dm_only: If True, only execute in Direct Messages (default: True)
        """
        return await self.execute_slash_command("curse", target_user, dm_only=dm_only)
    
    async def execute_stats(self, dm_only: bool = True) -> bool:
        """
        Execute /stats command.
        
        Args:
            dm_only: If True, only execute in Direct Messages (default: True)
        """
        return await self.execute_slash_command("stats", dm_only=dm_only)
    
    async def execute_journey(self, dm_only: bool = True) -> bool:
        """
        Execute /journey command.
        
        Args:
            dm_only: If True, only execute in Direct Messages (default: True)
        """
        return await self.execute_slash_command("journey", dm_only=dm_only)
