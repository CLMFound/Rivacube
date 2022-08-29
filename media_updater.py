import os
import pandas as pd
import time
import sys

from _modules.ConnectionManager import MediaManager

import enlighten


def get_all_files():
    return os.listdir(os.getcwd() + BASE_FOLDER)


if __name__ == '__main__':
    man = MediaManager(sys.argv[1] if len(sys.argv) > 1 else "remote")

    if sys.argv[1] == "local":
        BASE_FOLDER = "/home/ubuntu/drive/ICMAbox/Shared/RIVACUBE/CSV media files"
        BASE_FINAL_FOLDER = "/home/ubuntu/drive/ICMAbox/Shared/RIVACUBE/CSV_done"
    else:
        BASE_FOLDER = "/CSV_Media"
        BASE_FINAL_FOLDER = "/CSV_done"

    files = get_all_files()
    values = len(files)

    df = pd.DataFrame()

    # Creation du dataframe
    print("Creation du dataframe en cours")
    s = time.time()
    with enlighten.Counter(total=values, desc="", unit="fichier") as pbar:
        for i, file in enumerate(files):
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

    print("Importation des données dans la base en cours")
    s = time.time()
    errors = man.add_dataframe_to_database(df)
    e = time.time()

    t = e - s
    time_per_file = t / df.shape[0]

    print("Importations effectuées en {:.2f}s pour {} lignes, ({:.2f}s par ligne)".format(
        t, df.shape[0], time_per_file))

    man.close()

    if errors == 0:
        s = time.time()
        with enlighten.Counter(total=values, desc="", unit="fichier") as pbar:
            for file in files():
                # Deplace les fichiers dans YYYY_M(M?)_D(D?)
                os.replace(BASE_FOLDER + "/file", BASE_FINAL_FOLDER +
                           "_".join(file.split("_")[:-3]) + "/file")
                pbar.update()
        e = time.time()

        print(
            "Le transfert des fichiers s'est effectué en {:.2f}s".format(e - s))
