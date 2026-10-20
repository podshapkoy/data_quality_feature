import os
from contextlib import contextmanager

@contextmanager
def connection():
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as conn:
        yield conn
