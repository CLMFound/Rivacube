import datetime
import yfinance as yf
import pandas as pd
import time
import enlighten

import os
import sys

from _modules.ConnectionManager import TickerManager


def get_data_from_ticker(ticker: str, period: str = "1970-01-01") -> pd.DataFrame:
    data = yf.Ticker(ticker).history(start=period)
    return data


def add_file_to_database(data: pd.DataFrame, ticker: str) -> None:
    # Transformation de l'index (Date) en colonne
    data['date'] = data.index
    data['ticker'] = ticker
    data.dropna(inplace=True)

    # Suppression des lignes contenant des dates dupliquées, en ne gardant que la premiere
    # data.drop_duplicates(subset='date', keep='first')

    man.add_dataframe_to_database(data)


if __name__ == "__main__":
    # Utilise le type envoyé en CLI si présent, sinon utilise remote
    man = TickerManager(sys.argv[1] if len(sys.argv) > 1 else "remote")

    # Recupere les tickers et dates depuis la base
    # Filtre pour ne garder que ceux n'ayant pas ete mis a jour aujourd'hui (Pour ne pas recuperer 2 fois les memes valeurs)
    ticker_date = list(
        filter(lambda x: x[1] < datetime.date.today() - datetime.timedelta(days=1), man.get_all_tickers_and_last_date()))

    s = time.time()
    with enlighten.Counter(total=len(ticker_date), desc="", unit="ticker") as pbar:
        for ticker, da in ticker_date:
            add_file_to_database(get_data_from_ticker(
                ticker, (da + datetime.timedelta(days=1, hours=1)).strftime('%Y-%m-%d')), ticker)
            pbar.update()
    e = time.time()

    t = e - s
    time_per_file = t / len(ticker_date)
    print("Récupérations effectuées en {:.2f}s ({:.2f}s par fichier)".format(
        t, time_per_file))

    man.close()
