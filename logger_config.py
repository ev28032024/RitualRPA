"""
Logger Configuration
Provides centralized logging configuration for the entire application
"""
import logging
import sys
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional


# Default log settings
DEFAULT_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
DEFAULT_BACKUP_COUNT = 3


def setup_logger(
    name: str = "RitualRPA",
    log_to_file: bool = True,
    log_level: int = logging.INFO,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    log_dir: str = "logs"
) -> logging.Logger:
    """
    Setup and configure logger with both console and rotating file handlers
    
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
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(log_level)
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(message)s'  # Simple format for console
    )
    
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
        # Create logs directory if it doesn't exist
        logs_dir = Path(log_dir)
        logs_dir.mkdir(exist_ok=True)
        
        # Create log file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = logs_dir / f"ritual_rpa_{timestamp}.log"
        
        # Use RotatingFileHandler for automatic log rotation
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)  # More verbose in file
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"📝 Logging to file: {log_file}")
        logger.debug(f"Log rotation: max {max_bytes/1024/1024:.1f}MB, {backup_count} backups")
    
    return logger


def get_logger(name: str = "RitualRPA") -> logging.Logger:
    """
    Get existing logger or create a new one
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def set_log_level(level: int, name: str = "RitualRPA") -> None:
    """
    Change log level for an existing logger
    
    Args:
        level: New logging level (e.g., logging.DEBUG)
        name: Logger name
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Also update handler levels
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.setLevel(level)
