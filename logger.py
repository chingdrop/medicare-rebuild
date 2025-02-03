import logging
import colorlog
from pathlib import Path


def setup_logger(name: str, level: str='warning') -> logging.Logger:
    
    log_level = {
        'critical': logging.CRITICAL,
        'error': logging.ERROR,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG
    }
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    logger.setLevel(log_level[level])

    file_handler = logging.FileHandler(Path.cwd() / 'logs' / f'{name}_logfile.log')
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