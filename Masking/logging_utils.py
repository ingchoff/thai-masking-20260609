from loguru import logger
import os
import sys
import logging
from dotenv import load_dotenv

class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Get corresponding Loguru level
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
            
        # Find caller from original frame
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        # Get the best available name
        extra = getattr(record, 'extra', {})
        name = extra.get('service_name', 'api')

        # Create a new logger with the name binding
        bound_logger = logger.bind(service_name=name)
        bound_logger.opt(
            depth=depth,
            exception=record.exc_info
        ).log(level, record.getMessage())

load_dotenv()

LOG_ROTATION_SIZE = os.getenv("LOG_ROTATION_SIZE", "50MB")
LOG_RETENTION_DAYS = f"{int(os.getenv('LOG_RETENTION_DAYS', 30))} days"

def setup_logging():
    log_dir = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    logger.remove()

    # Intercept standard logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    
    # Disable UVicorn access logger
    logging.getLogger("uvicorn.access").handlers = []

    # Format that uses the service_name we bound
    base_format = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {extra[service_name]}:{function}:{line} - {message}"

    # Service logs
    logger.add(
        os.path.join(log_dir, "services.log"),
        rotation=LOG_ROTATION_SIZE,
        retention=LOG_RETENTION_DAYS, 
        level="INFO",
        filter=lambda r: r["extra"].get("type") != "pipeline" and r["level"].no < 40,
        format=base_format,
        enqueue=True
    )

    # Pipeline logs
    logger.add(
        os.path.join(log_dir, "pipeline.log"),
        rotation=LOG_ROTATION_SIZE,
        retention=LOG_RETENTION_DAYS,
        level="INFO",
        filter=lambda r: r["extra"].get("type") == "pipeline" and r["level"].no < 40,
        format=base_format,
        enqueue=True
    )

    # Error logs
    logger.add(
        os.path.join(log_dir, "errors.log"), 
        rotation=LOG_ROTATION_SIZE,
        retention=LOG_RETENTION_DAYS,
        level="ERROR",
        format=base_format,
        enqueue=True
    )

    console_fmt = "<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | " \
                  "<cyan>{extra[service_name]}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    logger.add(
        sys.stdout,
        colorize=True,
        format=console_fmt,
        level="INFO",
        enqueue=True          # safe for multi-process / async
    )

    return logger.bind(service_name="logging_setup")

# Initialize logger
LOGGER = setup_logging()