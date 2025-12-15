"""
AdsPower API Client
Manages browser profiles through AdsPower's local API
"""
import asyncio
import functools
import aiohttp
import requests
from typing import Dict, Optional, Any, Callable, TypeVar
from dataclasses import dataclass

# Type variable for retry decorator
T = TypeVar('T')


def async_retry(retries: int = 3, delay: float = 1.0, exceptions: tuple = (Exception,)):
    """
    Decorator for retrying async functions on failure
    
    Args:
        retries: Number of retry attempts
        delay: Delay between retries in seconds
        exceptions: Tuple of exceptions to catch and retry on
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            last_exception = None
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < retries - 1:
                        await asyncio.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


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


class AdsPowerAPI:
    """Client for interacting with AdsPower local API"""
    
    def __init__(self, api_url: str = "http://localhost:50325"):
        """
        Initialize AdsPower API client
        
        Args:
            api_url: AdsPower local API URL (default: http://localhost:50325)
        """
        self.api_url = api_url.rstrip('/')
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self) -> 'AdsPowerAPI':
        """Async context manager entry"""
        await self._get_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit"""
        await self.close()
        
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session for async requests"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self) -> None:
        """Close the aiohttp session"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        
    def check_connection(self) -> bool:
        """
        Check if AdsPower is running and accessible (synchronous)
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            response = requests.get(f"{self.api_url}/api/v1/browser/active", timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"❌ Cannot connect to AdsPower: {e}")
            return False
    
    async def check_connection_async(self) -> bool:
        """
        Check if AdsPower is running and accessible (asynchronous)
        
        Returns:
            bool: True if connection successful, False otherwise
        """
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
    
    async def start_browser(
        self, 
        profile_id: Optional[str] = None, 
        serial_number: Optional[int] = None,
        retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Start a browser profile with retries (non-blocking async)
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            retries: Number of retries
            
        Note: Either profile_id or serial_number must be provided.
              If both are provided, serial_number takes precedence.
            
        Returns:
            dict: Browser connection details (cdp_url) or None
        """
        # Validate that at least one identifier is provided
        if not profile_id and serial_number is None:
            print("❌ Either profile_id or serial_number must be provided")
            return None
        
        if profile_id and not profile_id.strip():
            profile_id = None
        
        if not profile_id and serial_number is None:
            print("❌ Either profile_id or serial_number must be provided")
            return None
        
        if retries < 1:
            print("❌ Retries must be at least 1")
            return None
        
        # Build params based on identifier type
        if serial_number is not None:
            params = {"serial_number": str(serial_number), "open_tabs": "1"}
            display_id = f"#{serial_number}"
        else:
            params = {"user_id": profile_id, "open_tabs": "1"}
            display_id = profile_id
        
        session = await self._get_session()
        
        for attempt in range(retries):
            try:
                url = f"{self.api_url}/api/v1/browser/start"
                
                if attempt > 0:
                    print(f"   ⚠️ Retry {attempt+1}/{retries} starting browser...")
                    await asyncio.sleep(2)
                
                # Use aiohttp for non-blocking request
                async with session.get(
                    url, 
                    params=params, 
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    data = await response.json()
                
                if data.get("code") == 0:
                    ws_url = data["data"].get("ws", {}).get("puppeteer", "")
                    debug_port = data["data"].get("debug_port")
                    
                    # Build CDP URL for Patchright connection
                    if ws_url:
                        cdp_url = ws_url
                    elif debug_port:
                        cdp_url = f"http://127.0.0.1:{debug_port}"
                    else:
                        webdriver_url = data["data"].get("webdriver", "")
                        if webdriver_url:
                            port = webdriver_url.split(":")[-1]
                            cdp_url = f"http://127.0.0.1:{port}"
                        else:
                            print("❌ No connection info available")
                            return None
                    
                    # Get actual profile_id from response if we used serial_number
                    actual_profile_id = profile_id
                    if serial_number is not None:
                        actual_profile_id = data["data"].get("id", f"serial_{serial_number}")
                    
                    print(f"✅ Browser started for profile: {display_id}")
                    print(f"   CDP URL: {cdp_url}")
                    
                    browser_info = BrowserInfo(
                        cdp_url=cdp_url,
                        ws_url=ws_url,
                        debug_port=debug_port,
                        profile_id=actual_profile_id
                    )
                    return browser_info.to_dict()
                else:
                    print(f"❌ Failed to start browser (Attempt {attempt+1}): {data.get('msg', 'Unknown error')}")
            
            except asyncio.TimeoutError:
                print(f"❌ Timeout starting browser (Attempt {attempt+1})")
            except aiohttp.ClientError as e:
                print(f"❌ Error starting browser (Attempt {attempt+1}): {e}")
            except Exception as e:
                print(f"❌ Unexpected error starting browser (Attempt {attempt+1}): {e}")
        
        return None
    
    async def stop_browser_async(
        self, 
        profile_id: Optional[str] = None, 
        serial_number: Optional[int] = None,
        retries: int = 2
    ) -> bool:
        """
        Stop a browser profile (non-blocking async) with retry logic
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            retries: Number of retry attempts on failure
            
        Note: Either profile_id or serial_number must be provided.
              If both are provided, serial_number takes precedence.
            
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        # Validate that at least one identifier is provided
        if not profile_id and serial_number is None:
            print("❌ Either profile_id or serial_number must be provided")
            return False
        
        if profile_id and not profile_id.strip():
            profile_id = None
        
        if not profile_id and serial_number is None:
            print("❌ Either profile_id or serial_number must be provided")
            return False
        
        # Build params based on identifier type
        if serial_number is not None:
            params = {"serial_number": str(serial_number)}
            display_id = f"#{serial_number}"
        else:
            params = {"user_id": profile_id}
            display_id = profile_id
        
        for attempt in range(retries):
            try:
                session = await self._get_session()
                url = f"{self.api_url}/api/v1/browser/stop"
                
                async with session.get(
                    url, 
                    params=params, 
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    data = await response.json()
                
                if data.get("code") == 0:
                    print(f"✅ Browser stopped for profile: {display_id}")
                    return True
                else:
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
        Stop a browser profile (synchronous, for backward compatibility)
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            
        Note: Either profile_id or serial_number must be provided.
              If both are provided, serial_number takes precedence.
            
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        # Validate that at least one identifier is provided
        if not profile_id and serial_number is None:
            print("❌ Either profile_id or serial_number must be provided")
            return False
        
        if profile_id and not profile_id.strip():
            profile_id = None
        
        if not profile_id and serial_number is None:
            print("❌ Either profile_id or serial_number must be provided")
            return False
        
        # Build params based on identifier type
        if serial_number is not None:
            params = {"serial_number": str(serial_number)}
            display_id = f"#{serial_number}"
        else:
            params = {"user_id": profile_id}
            display_id = profile_id
        
        try:
            url = f"{self.api_url}/api/v1/browser/stop"
            
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                print(f"✅ Browser stopped for profile: {display_id}")
                return True
            else:
                print(f"⚠️ Failed to stop browser: {data.get('msg', 'Unknown error')}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error stopping browser: {e}")
            return False
    
    def get_profile_status(
        self, 
        profile_id: Optional[str] = None, 
        serial_number: Optional[int] = None
    ) -> Optional[str]:
        """
        Get the status of a browser profile
        
        Args:
            profile_id: AdsPower profile ID (user_id)
            serial_number: AdsPower profile serial number (numeric)
            
        Note: Either profile_id or serial_number must be provided.
              If both are provided, serial_number takes precedence.
            
        Returns:
            str: Status ('Active' or 'Inactive') or None if failed
        """
        # Validate that at least one identifier is provided
        if not profile_id and serial_number is None:
            return None
        
        if profile_id and not profile_id.strip():
            profile_id = None
        
        if not profile_id and serial_number is None:
            return None
        
        # Build params based on identifier type
        if serial_number is not None:
            params = {"serial_number": str(serial_number)}
        else:
            params = {"user_id": profile_id}
        
        try:
            url = f"{self.api_url}/api/v1/browser/active"
            
            response = requests.get(url, params=params, timeout=5)
            data = response.json()
            
            if data.get("code") == 0:
                return data["data"]["status"]
            return None
            
        except requests.exceptions.RequestException:
            return None
