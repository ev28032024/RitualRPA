"""
Logger Configuration
Provides centralized logging configuration for the entire application
"""
import logging
import sys
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict

# Default log settings
DEFAULT_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
DEFAULT_BACKUP_COUNT = 3

# Cache for configured loggers to prevent duplicate handlers
_configured_loggers: Dict[str, logging.Logger] = {}
_log_file_path: Optional[Path] = None


def setup_logger(
    name: str = "RitualRPA",
    log_to_file: bool = True,
    log_level: int = logging.INFO,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    log_dir: str = "logs"
) -> logging.Logger:
    """
    Setup and configure logger with both console and rotating file handlers.
    
    Uses singleton pattern - subsequent calls with same name return existing logger.
    
    Args:
        name: Logger name
        log_to_file: Whether to log to file
        log_level: Logging level (default: INFO)
        max_bytes: Maximum log file size before rotation (default: 5MB)
        backup_count: Number of backup files to keep (default: 3)
        log_dir: Directory for log files (default: "logs")
        
    Returns:
        Configured logger instance
    """
    global _log_file_path
    
    # Return cached logger if already configured
    if name in _configured_loggers:
        return _configured_loggers[name]
    
    logger = logging.getLogger(name)
    
    # Clear any existing handlers to prevent duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    logger.setLevel(log_level)
    
    # Prevent propagation to root logger (avoids duplicate output)
    logger.propagate = False
    
    # Create formatters
    console_formatter = logging.Formatter('%(message)s')
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation (optional)
    if log_to_file:
        logs_dir = Path(log_dir)
        logs_dir.mkdir(exist_ok=True)
        
        # Reuse existing log file for current session
        if _log_file_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            _log_file_path = logs_dir / f"ritual_rpa_{timestamp}.log"
        
        file_handler = RotatingFileHandler(
            _log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Only print log file message once (for first logger)
        if len(_configured_loggers) == 0:
            print(f"\n📝 Logging to file: {_log_file_path}\n")
    
    # Cache the configured logger
    _configured_loggers[name] = logger
    
    return logger


def get_logger(name: str = "RitualRPA") -> logging.Logger:
    """
    Get existing logger or create a new one.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    if name in _configured_loggers:
        return _configured_loggers[name]
    return setup_logger(name)


def set_log_level(level: int, name: Optional[str] = None) -> None:
    """
    Change log level for an existing logger.
    
    Args:
        level: New logging level (e.g., logging.DEBUG)
        name: Logger name (None = all configured loggers)
    """
    if name:
        loggers = [logging.getLogger(name)] if name in _configured_loggers else []
    else:
        loggers = list(_configured_loggers.values())
    
    for logger in loggers:
        logger.setLevel(level)
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.setLevel(level)


def get_log_file_path() -> Optional[Path]:
    """Get the current log file path."""
    return _log_file_path


def reset_loggers() -> None:
    """Reset all loggers (useful for testing)."""
    global _configured_loggers, _log_file_path
    
    for logger in _configured_loggers.values():
        logger.handlers.clear()
    
    _configured_loggers.clear()
    _log_file_path = None
