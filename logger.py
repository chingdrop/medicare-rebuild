import logging
import colorlog


def setup_logger(name: str, level: int=3) -> logging.Logger:
    if not isinstance(level, int) and (level >= 1 and level <= 5):
        raise ValueError("Level is not valid integer range.")
    
    log_level = {
        1: logging.CRITICAL,
        2: logging.ERROR,
        3: logging.WARNING,
        4: logging.INFO,
        5: logging.DEBUG
    }
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    logger.setLevel(log_level[level])

    file_handler = logging.FileHandler(f'{name}_logfile.log')
    file_handler.setLevel(log_level[level])

    stream_handler = colorlog.StreamHandler()
    stream_handler.setLevel(log_level[level])

    formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'blue',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'magenta',
        }
    )

    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger