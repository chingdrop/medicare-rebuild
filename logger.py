import logging
import colorlog


def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # File handler for logging to a file
    file_handler = logging.FileHandler(f'{name}_logfile.log')
    file_handler.setLevel(logging.DEBUG)

    # Colorlog stream handler for terminal output with colors
    stream_handler = colorlog.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)

    # Define a colorful formatter for terminal output
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

    # Attach the formatter to both handlers
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    stream_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger