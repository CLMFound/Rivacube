import os
import psycopg2
import pandas as pd
import time

from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

import enlighten

load_dotenv()  # Charge les informations depuis le fichier .env

PORT = int(os.getenv("PORT"))
REMOTE_HOST = os.getenv("REMOTE_HOST")
REMOTE_SSH_PORT = int(os.getenv("REMOTE_SSH_PORT"))
REMOTE_USERNAME = os.getenv("REMOTE_USERNAME")
SSH_PASSWORD = os.getenv("SSH_KEY_PASSWORD")
SSH_PKEY = os.getenv("SSH_PKEY")

# Creation d'un tunnel SSH pour accéder a la base de donnée distante
server = SSHTunnelForwarder(
    (REMOTE_HOST, REMOTE_SSH_PORT),
    ssh_username=REMOTE_USERNAME,
    ssh_pkey=SSH_PKEY,
    ssh_private_key_password=SSH_PASSWORD,
    remote_bind_address=('localhost', PORT),
    local_bind_address=('localhost', PORT),
    compression=False)
server.start()

# Connexion a la base de donnée locale
conn = psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
                        host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"))

BASE_FOLDER = "/CSV_Media"


def get_all_files():
    return os.listdir(os.getcwd() + BASE_FOLDER)


def add_file_to_database(df: pd.DataFrame):
    cur = conn.cursor()

    # Suppression des lignes dupliquées
    df.drop_duplicates(subset=['status_id'], keep='first', inplace=True)

    df.dropna(subset=["created_at"], inplace=True)

    l = df.shape[0]

    df.reset_index(inplace=True)

    df.drop(columns=["index", "Unnamed: 0"], inplace=True)

    # Verification de la valeur max
    #df["mentions_screen_name"] = df["reply_to_screen_name"].str.len()

    #print(df.sort_values("retweet_count", ascending=False).retweet_count.head())

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

            cur.execute("""
                INSERT INTO media_media (status_id, user_id, created_at, screen_name, text, source, display_text_width, reply_to_status_id, reply_to_user_id, reply_to_screen_name, is_quote, is_retweet, favorite_count, retweet_count, mentions_screen_name, lang, description, htag)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
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
        conn.commit()


values = len(get_all_files())


delete = input("Y pour supprimer la table media ?\n")
if delete == "Y":
    print("Suppression de la table media")
    curs = conn.cursor()
    curs.execute("DELETE FROM media_media")
    conn.commit()
    curs.close()
else:
    print("La table media ne sera pas supprimée")

df = pd.DataFrame()

s = time.time()
with enlighten.Counter(total=values, desc="", unit="fichier") as pbar:
    for i, file in enumerate(get_all_files()):
        if file != ".DS_Store":
            dfT = pd.DataFrame(pd.read_csv(
                r'./CSV_Media/{}'.format(file), sep=";"))
            dfT["htag"] = file.split("_")[0]
            df = pd.concat([df, dfT])
        pbar.update()
e = time.time()

t = e - s
time_per_file = t / values

print("Recuperation des CSV effectuées en {:.2f}s pour {} fichiers, ({:.2f}s par fichier, {:.2f} minutes pour 1500)".format(
    t, values, time_per_file, time_per_file * 25))

s = time.time()
add_file_to_database(df)
e = time.time()

t = e - s
time_per_file = t / df.shape[0]

print("Importations effectuées en {:.2f}s pour {} lignes, ({:.2f}s par ligne, {:.2f} minutes pour 500000)".format(
    t, df.shape[0], time_per_file, time_per_file * 8333))

conn.close()
