"""
Structured logging configuration for Banking Data AI Platform

Provides enterprise-grade logging with:
- JSON formatting for log aggregation
- Context propagation
- Performance tracking
- Error tracking with stack traces
"""

import logging
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from functools import wraps
import time

class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info)
            }
        
        # Add custom fields from extra
        if hasattr(record, "custom_fields"):
            log_data.update(record.custom_fields)
        
        return json.dumps(log_data)

def setup_logging(
    level: str = "INFO",
    json_format: bool = True,
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Setup structured logging configuration
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Use JSON formatting if True, else use standard format
        log_file: Optional log file path
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("banking_data_ai")
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    logger.handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    
    if json_format:
        console_handler.setFormatter(JSONFormatter())
    else:
        console_handler.setFormatter(
            logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - %(message)s'
            )
        )
    
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JSONFormatter() if json_format else logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(file_handler)
    
    return logger

def log_execution_time(logger: Optional[logging.Logger] = None):
    """
    Decorator to log function execution time
    
    Usage:
        @log_execution_time(logger)
        def my_function():
            pass
    """
    if logger is None:
        logger = logging.getLogger("banking_data_ai")
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            function_name = f"{func.__module__}.{func.__name__}"
            
            logger.info(
                f"Starting {function_name}",
                extra={"custom_fields": {"function": function_name, "event": "start"}}
            )
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                logger.info(
                    f"Completed {function_name}",
                    extra={"custom_fields": {
                        "function": function_name,
                        "event": "complete",
                        "execution_time_seconds": round(execution_time, 3)
                    }}
                )
                
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                
                logger.error(
                    f"Failed {function_name}: {str(e)}",
                    exc_info=True,
                    extra={"custom_fields": {
                        "function": function_name,
                        "event": "error",
                        "execution_time_seconds": round(execution_time, 3),
                        "error_type": type(e).__name__
                    }}
                )
                raise
        
        return wrapper
    return decorator

class LogContext:
    """
    Context manager for adding contextual information to logs
    
    Usage:
        with LogContext(logger, customer_id="CUST-123", action="fraud_check"):
            logger.info("Checking fraud")
    """
    
    def __init__(self, logger: logging.Logger, **context):
        self.logger = logger
        self.context = context
        self.old_factory = None
    
    def __enter__(self):
        old_factory = logging.getLogRecordFactory()
        
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            if not hasattr(record, "custom_fields"):
                record.custom_fields = {}
            record.custom_fields.update(self.context)
            return record
        
        self.old_factory = old_factory
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.old_factory:
            logging.setLogRecordFactory(self.old_factory)

# Example usage
if __name__ == "__main__":
    # Setup logging
    logger = setup_logging(level="INFO", json_format=True)
    
    # Simple logging
    logger.info("Application started")
    
    # Logging with context
    with LogContext(logger, customer_id="CUST-123", transaction_id="TXN-456"):
        logger.info("Processing transaction")
    
    # Function with execution time logging
    @log_execution_time(logger)
    def process_data():
        time.sleep(1)
        return "Done"
    
    result = process_data()

