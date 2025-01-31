from pathlib import Path


def read_sql_file(filepath: Path) -> str:
    with open(filepath, 'r') as f:
        sql_query = f.read()
    return sql_query