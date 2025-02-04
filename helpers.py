from pathlib import Path


def read_file(filepath: Path, encoding: str=None) -> str:
    """Reads a file into text with given encoding.
        
        Args:
            - filename (Path): The filepath using pathlib.
            - encoding (str): The name of an encoding library.
        
        Returns:
            - String: The contents of the file.
        """
    with open(filepath, 'r', encoding=encoding) as f:
        file = f.read()
    return file