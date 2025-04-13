"""
Logger module to configure consistent logging across the application.
"""
import os
import logging
from ..config.config import config

def get_logger(name):
    """
    Get a configured logger with the specified name.
    Each module should use its own logger for better traceability.
    
    Args:
        name: The name of the logger, usually __name__
        
    Returns:
        A configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure the logger if it doesn't have handlers already
    if not logger.handlers:
        # Set the log level from configuration
        log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
        logger.setLevel(log_level)
        
        # Create formatter
        formatter = logging.Formatter(config.LOG_FORMAT)
        
        # Add file handler if log file is configured
        if config.LOG_FILE:
            # Ensure log directory exists
            os.makedirs(os.path.dirname(config.LOG_FILE), exist_ok=True)
            
            file_handler = logging.FileHandler(config.LOG_FILE)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Prevent propagation to avoid duplicate logs
        logger.propagate = False
    
    return logger