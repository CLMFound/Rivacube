import os
import pandas as pd
import time
import sys

from _modules.ConnectionManager import MediaManager

import enlighten


def get_all_files():
    return os.listdir(os.getcwd() + BASE_FOLDER)


if __name__ == '__main__':
    BASE_FOLDER = "/CSV_Media"

    man = MediaManager(sys.argv[1] if len(sys.argv) > 1 else "remote")

    values = len(get_all_files())

    df = pd.DataFrame()

    # Creation du dataframe
    s = time.time()
    with enlighten.Counter(total=values, desc="", unit="fichier") as pbar:
        for i, file in enumerate(get_all_files()):
            if file != ".DS_Store":
                dfT = pd.DataFrame(pd.read_csv(
                    r'.{}/{}'.format(BASE_FOLDER, file), sep=";"))
                dfT["htag"] = file.split("_")[0]
                df = pd.concat([df, dfT])
            pbar.update()
    e = time.time()

    t = e - s
    time_per_file = t / values

    print("Recuperation des CSV effectuées en {:.2f}s pour {} fichiers, ({:.2f}s par fichier)".format(
        t, values, time_per_file))

    s = time.time()
    errors = man.add_dataframe_to_database(df)
    e = time.time()

    t = e - s
    time_per_file = t / df.shape[0]

    print("Importations effectuées en {:.2f}s pour {} lignes, ({:.2f}s par ligne)".format(
        t, df.shape[0], time_per_file))

    man.close()

    if errors == 0:
        # TODO: Des que j'aurai les droits sur le kdrive, creer le script de deplacement des fichiers par date
        pass
