import pandas
import psycopg2.errors
import psycopg2.extensions
import sshtunnel
from typing import Literal


class ConnectionManager:
    REMOTE_HOST: str
    REMOTE_SSH_PORT: int
    REMOTE_USERNAME: str
    SSH_PASSWORD: str
    SSH_PKEY: str
    DB_NAME: str
    DB_USER: str
    DB_HOST: str
    DB_PORT: int
    type: Literal['local', 'remote']
    conn: psycopg2.extensions.connection
    server: sshtunnel.SSHTunnelForwarder
    def __init__(self, type: Literal['local', 'remote'] = ...) -> None: ...
    def __del__(self) -> None: ...
    def close(self) -> None: ...
    def connect(self) -> psycopg2.extensions.connection: ...
    def start_server(self) -> sshtunnel.SSHTunnelForwarder: ...


class TickerManager(ConnectionManager):
    def add_dataframe_to_database(self, df: pandas.DataFrame) -> None: ...
    def get_all_tickers_and_last_date(self) -> list[tuple]: ...
