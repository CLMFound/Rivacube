import array
from dotenv import load_dotenv
import os
import psycopg2
import psycopg2.extensions
import psycopg2.errors
import sshtunnel
import enlighten

import pandas as pd

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
    def add_dataframe_to_database(self, df: pd.DataFrame) -> None:
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


class MediaManager(ConnectionManager):
    def get_list_of_status_id(self) -> list[int]:
        print("Recuperation de liste de status id en cours...")
        cur = self.conn.cursor()
        cur.execute("SELECT status_id FROM {}".format(os.getenv("DB_MEDIA")))

        # Transforme la liste de tuples en simple liste
        return [x[0] for x in cur.fetchall()]

    def add_dataframe_to_database(self, df: pd.DataFrame) -> int:
        cur = self.conn.cursor()

        # Suppression des lignes dupliquées
        df.drop_duplicates(subset=['status_id'], keep='first', inplace=True)

        # Suppression des lignes ne contenant pas de date
        df.dropna(subset=["created_at"], inplace=True)

        df.reset_index(inplace=True)

        df.drop(columns=["index", "Unnamed: 0"], inplace=True)

        # Ne garde dans le dataframe que les lignes ne contenant pas de status id deja present dans la base de donnée
        df = df[~df['status_id'].isin(self.get_list_of_status_id())]

        # Recuperation du nombre de lignes
        l = df.shape[0]

        # Verification de la valeur max
        #df["mentions_screen_name"] = df["reply_to_screen_name"].str.len()

        #print(df.sort_values("retweet_count", ascending=False).retweet_count.head())

        errors = 0
        # Importation des données
        with enlighten.Counter(total=l, desc="", unit="ligne") as pbar2:
            for row in df.itertuples():
                pbar2.update()

                reply_to_status_id = None if pd.isna(
                    row.reply_to_status_id) else row.reply_to_status_id

                reply_to_user_id = None if pd.isna(
                    row.reply_to_user_id) else row.reply_to_user_id

                reply_to_screen_name = None if pd.isna(
                    row.reply_to_screen_name) else row.reply_to_screen_name

                if pd.isna(row.mentions_screen_name):
                    mentions_screen_name = None
                elif row.mentions_screen_name[:2] == "c(":
                    mentions_screen_name = row.mentions_screen_name[3:-2].split(
                        "\", \"")
                else:
                    mentions_screen_name = [row.mentions_screen_name]

                description = None if pd.isna(
                    row.description) else row.description

                try:
                    cur.execute("""
                        INSERT INTO {} (status_id, user_id, created_at, screen_name, text, source, display_text_width, reply_to_status_id, reply_to_user_id, reply_to_screen_name, is_quote, is_retweet, favorite_count, retweet_count, mentions_screen_name, lang, description, htag)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """.format(os.getenv("DB_MEDIA")), (
                        row.status_id,
                        row.user_id,
                        row.created_at,
                        row.screen_name,
                        row.text,
                        row.source,
                        row.display_text_width,
                        reply_to_status_id,
                        reply_to_user_id,
                        reply_to_screen_name,
                        row.is_quote,
                        row.is_retweet,
                        row.favorite_count,
                        row.retweet_count,
                        mentions_screen_name,
                        row.lang,
                        description,
                        row.htag
                    )
                    )
                except psycopg2.errors as e:
                    self.conn.rollback()
                    print("L'importation de {}\nRaison: {}".format(
                        row.status_id, e))
                    errors += 1
            self.conn.commit()
        return errors
