from dotenv import load_dotenv
import os
import psycopg2
import psycopg2.extensions
import psycopg2.errors
import sshtunnel

import pandas

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
    type: Literal["local", "remote"]
    conn: psycopg2.extensions.connection
    server: sshtunnel.SSHTunnelForwarder

    def __init__(self, type: Literal["local", "remote"] = "local") -> None:
        load_dotenv()  # Charge les information depuis le fichier .env

        # Adresse du serveur distant
        self.REMOTE_HOST: str = os.getenv("REMOTE_HOST") or ""
        # Port SSH du serveur distant
        self.REMOTE_SSH_PORT: int = int(
            os.getenv("REMOTE_SSH_PORT") or "0") or 0
        # Nom d'utilisateur du serveur distant
        self.REMOTE_USERNAME = os.getenv("REMOTE_USERNAME") or ""
        # Passphrase SSH
        self.SSH_PASSWORD: str = os.getenv("SSH_KEY_PASSWORD") or ""
        # Emplacement de la cle SSH
        self.SSH_PKEY: str = os.getenv("SSH_PKEY") or ""
        # Nom de la base de donnée
        self.DB_NAME: str = os.getenv("DB_NAME") or ""
        # Nom d'utilisateur de la base de donnée
        self.DB_USER: str = os.getenv("DB_USER") or ""
        # Adresse locale de la base de donnée
        self.DB_HOST: str = os.getenv("DB_HOST") or ""
        # Port local de la base de donnée
        self.DB_PORT: int = int(os.getenv("DB_PORT") or "0") or 0

        # Type de connexion a utiliser: local ou remote
        self.type = type

        if self.type == "remote":
            self.server = self.start_server()

        self.conn = self.connect()

    # A la suppression de l'instance, lance la méthode close
    def __del__(self) -> None:
        self.close()

    # Ferme la connexion et stop le serveur si lancé
    def close(self) -> None:
        self.conn.close()
        print("La connexion à la base de donnée est interrompue")
        if self.type == "remote":
            self.server.stop()
            print("Le serveur est stoppé")

    # Connecte et renvoie psycopg2.extensions.connection
    def connect(self) -> psycopg2.extensions.connection:
        print("Connexion à la base de donnée en cours")
        c = psycopg2.connect(
            database=self.DB_NAME,
            user=self.DB_USER,
            host=self.DB_HOST,
            port=self.DB_PORT
        )
        print("Connexion a la base de donnée établie")
        return c

    # Demarre le serveur SSH
    def start_server(self) -> sshtunnel.SSHTunnelForwarder:
        print("Lancement du serveur")
        server = sshtunnel.SSHTunnelForwarder(
            (self.REMOTE_HOST, self.REMOTE_SSH_PORT),
            ssh_username=self.REMOTE_USERNAME,
            ssh_pkey=self.SSH_PKEY,
            ssh_private_key_password=self.SSH_PASSWORD,
            remote_bind_address=('localhost', self.DB_PORT),
            local_bind_address=('localhost', self.DB_PORT),
            compression=False)

        server.start()
        print("Le serveur est lancé")
        return server


class TickerManager(ConnectionManager):
    def add_dataframe_to_database(self, df: pandas.DataFrame) -> None:
        cur = self.conn.cursor()

        # Importation des données
        for row in df.itertuples():
            try:
                cur.execute("""
                    INSERT INTO {} (id, yticker, date, px_last, px_high, px_low, px_open, px_volume)
                    VALUES ('{}','{}','{}',{},{},{},{},{})
                    """.format(
                    os.getenv('DB_TICKER'),
                    row.ticker +
                    "{:0>4d}{:0>2d}{:0>2d}".format(
                        row.date.year, row.date.month, row.date.day),
                    row.ticker,
                    row.date,
                    row.Close,
                    row.High,
                    row.Low,
                    row.Open,
                    row.Volume
                )
                )
            except psycopg2.errors.UniqueViolation as e:
                self.conn.rollback()
                print("L'importation de {} à la date de {} a echoué.\nRaison: {}".format(
                    row.ticker, row.date, e))

        self.conn.commit()
        cur.close()

    def get_all_tickers_and_last_date(self) -> list[tuple]:
        cur = self.conn.cursor()

        cur.execute("""
            SELECT yticker, MAX(date)
            FROM {}
            GROUP BY yticker
        """.format(os.getenv("DB_TICKER")))

        return cur.fetchall()
