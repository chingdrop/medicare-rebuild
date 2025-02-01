from pathlib import Path


def read_sql_file(filepath: Path, encoding=None) -> str:
    with open(filepath, 'r', encoding=encoding) as f:
        sql_query = f.read()
    return sql_query