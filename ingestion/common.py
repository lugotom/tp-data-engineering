"""
Utilitaires partagés par les scripts d'ingestion. `get_connection` est fourni tel quel ;
`fetch_csv` est à vous d'implémenter (cf. TODO)
"""
import os

import duckdb
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")
RAW_BASE = "https://raw.githubusercontent.com/kevinl75/tp-polytech-dataset/main"


def fetch_csv(subdir: str, filename: str) -> pd.DataFrame:
    """Renvoie le contenu de <subdir>/<filename> sous forme de DataFrame pandas.

    Si le fichier n'est pas encore dans bronze/, on le télécharge et on l'enregistre.
    Ensuite on le relit toujours depuis le disque : bronze/ est notre copie locale.
    """
    path = os.path.join(BRONZE_DIR, subdir, filename)

    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df = pd.read_csv(f"{RAW_BASE}/{subdir}/{filename}")
        df.to_csv(path, index=False)

    return pd.read_csv(path)


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)
