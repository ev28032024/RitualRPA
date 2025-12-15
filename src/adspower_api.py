"""
AdsPower API Client
Manages browser profiles through AdsPower's local API
"""
import asyncio
import aiohttp
import requests
from typing import Dict, Optional, Any, Tuple
from dataclasses import dataclass


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class BrowserInfo:
    """Browser connection information"""
    cdp_url: str
    ws_url: str
    debug_port: Optional[int]
    profile_id: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backward compatibility"""
        return {
            "cdp_url": self.cdp_url,
            "ws_url": self.ws_url,
            "debug_port": self.debug_port,
            "profile_id": self.profile_id
        }


@dataclass
class ProfileIdentifier:
    """Validated profile identifier"""
    profile_id: Optional[str]
    serial_number: Optional[int]
    display_name: str
    params: Dict[str, str]
    
    @classmethod
    def create(
        cls, 
        profile_id: Optional[str] = None, 
        serial_number: Optional[int] = None
    ) -> Optional["ProfileIdentifier"]:
        """
        Create a validated profile identifier.
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            
        Returns:
            ProfileIdentifier or None if validation fails
        """
        # Clean up profile_id
        if profile_id and not profile_id.strip():
            profile_id = None
        
        # Validate that at least one identifier is provided
        if not profile_id and serial_number is None:
            return None
        
        # Build params and display name based on identifier type
        if serial_number is not None:
            return cls(
                profile_id=None,
                serial_number=serial_number,
                display_name=f"#{serial_number}",
                params={"serial_number": str(serial_number)}
            )
        else:
            return cls(
                profile_id=profile_id,
                serial_number=None,
                display_name=profile_id,
                params={"user_id": profile_id}
            )


# ============================================================================
# API CLIENT
# ============================================================================

class AdsPowerAPI:
    """Client for interacting with AdsPower local API"""
    
    def __init__(self, api_url: str = "http://localhost:50325"):
        """
        Initialize AdsPower API client.
        
        Args:
            api_url: AdsPower local API URL (default: http://localhost:50325)
        """
        self.api_url = api_url.rstrip('/')
        self._session: Optional[aiohttp.ClientSession] = None
    
    # ========================================================================
    # CONTEXT MANAGER
    # ========================================================================
    
    async def __aenter__(self) -> 'AdsPowerAPI':
        await self._get_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
    
    # ========================================================================
    # SESSION MANAGEMENT
    # ========================================================================
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session for async requests."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
    
    # ========================================================================
    # CONNECTION CHECK
    # ========================================================================
    
    def check_connection(self) -> bool:
        """Check if AdsPower is running and accessible (synchronous)."""
        try:
            response = requests.get(
                f"{self.api_url}/api/v1/browser/active",
                timeout=5
            )
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"❌ Cannot connect to AdsPower: {e}")
            return False
    
    async def check_connection_async(self) -> bool:
        """Check if AdsPower is running and accessible (asynchronous)."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.api_url}/api/v1/browser/active",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                return response.status == 200
        except Exception as e:
            print(f"❌ Cannot connect to AdsPower: {e}")
            return False
    
    # ========================================================================
    # BROWSER START
    # ========================================================================
    
    async def start_browser(
        self, 
        profile_id: Optional[str] = None, 
        serial_number: Optional[int] = None,
        retries: int = 3,
        timeout: float = 30.0
    ) -> Optional[Dict[str, Any]]:
        """
        Start a browser profile with retries.
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            retries: Number of retries
            timeout: Request timeout in seconds
            
        Note: Either profile_id or serial_number must be provided.
              If both are provided, serial_number takes precedence.
            
        Returns:
            Browser connection details (cdp_url) or None
        """
        # Validate identifier
        identifier = ProfileIdentifier.create(profile_id, serial_number)
        if not identifier:
            print("❌ Either profile_id or serial_number must be provided")
            return None
        
        if retries < 1:
            retries = 1
        
        session = await self._get_session()
        
        for attempt in range(retries):
            try:
                if attempt > 0:
                    print(f"   ⚠️ Retry {attempt+1}/{retries} starting browser...")
                    await asyncio.sleep(2)
                
                result = await self._do_start_browser(
                    session, 
                    identifier, 
                    timeout,
                    attempt + 1
                )
                
                if result:
                    return result
                    
            except asyncio.TimeoutError:
                print(f"❌ Timeout starting browser (Attempt {attempt+1})")
            except aiohttp.ClientError as e:
                print(f"❌ Error starting browser (Attempt {attempt+1}): {e}")
            except Exception as e:
                print(f"❌ Unexpected error starting browser (Attempt {attempt+1}): {e}")
        
        return None
    
    async def _do_start_browser(
        self, 
        session: aiohttp.ClientSession,
        identifier: ProfileIdentifier,
        timeout: float,
        attempt: int
    ) -> Optional[Dict[str, Any]]:
        """Execute single browser start attempt."""
        url = f"{self.api_url}/api/v1/browser/start"
        params = {**identifier.params, "open_tabs": "1"}
        
        async with session.get(
            url, 
            params=params, 
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as response:
            data = await response.json()
        
        if data.get("code") != 0:
            print(f"❌ Failed to start browser (Attempt {attempt}): {data.get('msg', 'Unknown error')}")
            return None
        
        # Extract connection info
        browser_info = self._extract_browser_info(data, identifier)
        if browser_info:
            print(f"✅ Browser started for profile: {identifier.display_name}")
            print(f"   CDP URL: {browser_info['cdp_url']}")
            return browser_info
        
        print("❌ No connection info available")
        return None
    
    def _extract_browser_info(
        self, 
        data: Dict[str, Any], 
        identifier: ProfileIdentifier
    ) -> Optional[Dict[str, Any]]:
        """Extract browser connection info from API response."""
        browser_data = data.get("data", {})
        ws_url = browser_data.get("ws", {}).get("puppeteer", "")
        debug_port = browser_data.get("debug_port")
        
        # Build CDP URL from available info
        cdp_url = None
        if ws_url:
            cdp_url = ws_url
        elif debug_port:
            cdp_url = f"http://127.0.0.1:{debug_port}"
        else:
            webdriver_url = browser_data.get("webdriver", "")
            if webdriver_url:
                port = webdriver_url.split(":")[-1]
                cdp_url = f"http://127.0.0.1:{port}"
        
        if not cdp_url:
            return None
        
        # Get actual profile_id from response if we used serial_number
        actual_profile_id = identifier.profile_id
        if identifier.serial_number is not None:
            actual_profile_id = browser_data.get("id", f"serial_{identifier.serial_number}")
        
        return BrowserInfo(
            cdp_url=cdp_url,
            ws_url=ws_url,
            debug_port=debug_port,
            profile_id=actual_profile_id
        ).to_dict()
    
    # ========================================================================
    # BROWSER STOP
    # ========================================================================
    
    async def stop_browser_async(
        self, 
        profile_id: Optional[str] = None, 
        serial_number: Optional[int] = None,
        retries: int = 2,
        timeout: float = 15.0
    ) -> bool:
        """
        Stop a browser profile (non-blocking async) with retry logic.
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            retries: Number of retry attempts on failure
            timeout: Overall timeout for the operation in seconds
            
        Returns:
            True if stopped successfully
        """
        identifier = ProfileIdentifier.create(profile_id, serial_number)
        if not identifier:
            print("❌ Either profile_id or serial_number must be provided")
            return False
        
        try:
            return await asyncio.wait_for(
                self._do_stop_browser(identifier, retries),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            print(f"⚠️ Stop browser operation timed out after {timeout}s for {identifier.display_name}")
            return False
    
    async def _do_stop_browser(self, identifier: ProfileIdentifier, retries: int) -> bool:
        """Execute browser stop with retries."""
        url = f"{self.api_url}/api/v1/browser/stop"
        
        for attempt in range(retries):
            try:
                session = await self._get_session()
                
                async with session.get(
                    url, 
                    params=identifier.params, 
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    data = await response.json()
                
                if data.get("code") == 0:
                    print(f"✅ Browser stopped for profile: {identifier.display_name}")
                    return True
                
                error_msg = data.get('msg', 'Unknown error')
                if attempt < retries - 1:
                    print(f"⚠️ Failed to stop browser (attempt {attempt+1}): {error_msg}, retrying...")
                    await asyncio.sleep(1)
                else:
                    print(f"⚠️ Failed to stop browser: {error_msg}")
                    return False
                    
            except Exception as e:
                if attempt < retries - 1:
                    print(f"⚠️ Error stopping browser (attempt {attempt+1}): {e}, retrying...")
                    await asyncio.sleep(1)
                else:
                    print(f"❌ Error stopping browser: {e}")
                    return False
        
        return False
    
    def stop_browser(
        self, 
        profile_id: Optional[str] = None, 
        serial_number: Optional[int] = None
    ) -> bool:
        """
        Stop a browser profile (synchronous, for backward compatibility).
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            
        Returns:
            True if stopped successfully
        """
        identifier = ProfileIdentifier.create(profile_id, serial_number)
        if not identifier:
            print("❌ Either profile_id or serial_number must be provided")
            return False
        
        try:
            url = f"{self.api_url}/api/v1/browser/stop"
            response = requests.get(url, params=identifier.params, timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                print(f"✅ Browser stopped for profile: {identifier.display_name}")
                return True
            
            print(f"⚠️ Failed to stop browser: {data.get('msg', 'Unknown error')}")
            return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error stopping browser: {e}")
            return False
    
    # ========================================================================
    # BROWSER STATUS
    # ========================================================================
    
    def get_profile_status(
        self, 
        profile_id: Optional[str] = None, 
        serial_number: Optional[int] = None
    ) -> Optional[str]:
        """
        Get the status of a browser profile.
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            
        Returns:
            Status ('Active' or 'Inactive') or None if failed
        """
        identifier = ProfileIdentifier.create(profile_id, serial_number)
        if not identifier:
            return None
        
        try:
            url = f"{self.api_url}/api/v1/browser/active"
            response = requests.get(url, params=identifier.params, timeout=5)
            data = response.json()
            
            if data.get("code") == 0:
                return data["data"]["status"]
            return None
            
        except requests.exceptions.RequestException:
            return None
    
    async def get_profile_status_async(
        self, 
        profile_id: Optional[str] = None, 
        serial_number: Optional[int] = None
    ) -> Optional[str]:
        """
        Get the status of a browser profile (async version).
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            
        Returns:
            Status ('Active' or 'Inactive') or None if failed
        """
        identifier = ProfileIdentifier.create(profile_id, serial_number)
        if not identifier:
            return None
        
        try:
            session = await self._get_session()
            url = f"{self.api_url}/api/v1/browser/active"
            
            async with session.get(
                url, 
                params=identifier.params, 
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                data = await response.json()
            
            if data.get("code") == 0:
                return data["data"]["status"]
            return None
            
        except Exception:
            return None
