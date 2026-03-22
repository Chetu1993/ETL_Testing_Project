import logging
from datetime import datetime
def get_logging():
    logger=logging.getLogger("etl_logger")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.INFO)
    log_file=f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler=logging.FileHandler(log_file)
    formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.propagate=False

    return logger