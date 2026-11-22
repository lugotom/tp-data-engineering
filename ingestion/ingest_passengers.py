"""
À compléter : ingestion de l'entité `passengers`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import fetch_csv, get_connection


def _snapshot_file(lang: str, day: date = None, init: bool = False):
    if init:
        return "init", f"passengers_{lang}.csv"
    assert day is not None
    return "2025-09", f"passengers_{lang}_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie les deux snapshots passagers (EN et FR) du jour (ou de init/) dans bronze/."""
    for lang in ("en", "fr"):
        fetch_csv(*_snapshot_file(lang, day, init))


def create_silver_table(con):
    # Système anglais uniformisé: DATE pour toutes les dates, Male/Female
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_passengers (
            passenger_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            nationality VARCHAR,
            email VARCHAR,
            birth_date DATE,
            signup_date DATE,
            source_system VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit les 2 snapshots passagers (EN + FR) depuis bronze/, les harmonise et les upsert.

    Les deux fichiers viennent de deux CRM différents, avec des formats différents :
      EN : passenger_id, first_name, ..., gender = Male/Female, dates en YYYY-MM-DD
      FR : id_passager,  prenom, ..., genre  = Homme/Femme, dates en DD/MM/YYYY
    On les met au même format puis on les empile (UNION ALL) avant l'upsert.
    Les passenger_id ne se chevauchent pas entre EN et FR : pas de doublon à gérer.
    """
    df_en = fetch_csv(*_snapshot_file("en", day, init))
    df_fr = fetch_csv(*_snapshot_file("fr", day, init))
    snapshot_date = date(2025, 8, 31) if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot_en", df_en)
    con.register("snapshot_fr", df_fr)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE snapshot AS
        SELECT
            passenger_id,
            first_name,
            last_name,
            gender,
            nationality,
            email,
            CAST(birth_date AS DATE) AS birth_date,
            CAST(signup_date AS DATE) AS signup_date,
            'en' AS source_system
        FROM snapshot_en

        UNION ALL

        SELECT
            id_passager AS passenger_id,
            prenom AS first_name,
            nom AS last_name,
            CASE genre WHEN 'Homme' THEN 'Male' WHEN 'Femme' THEN 'Female' END AS gender,
            nationalite AS nationality,
            email,
            CAST(strptime(date_naissance, '%d/%m/%Y') AS DATE) AS birth_date,
            CAST(strptime(date_inscription, '%d/%m/%Y') AS DATE) AS signup_date,
            'fr' AS source_system
        FROM snapshot_fr
    """)

    con.execute("""
        INSERT INTO silver_passengers
        SELECT *, true, NULL, now(), now()   -- is_active, deleted_date, insert/update_timestamp
        FROM snapshot
        ON CONFLICT (passenger_id) DO UPDATE SET
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            gender = excluded.gender,
            nationality = excluded.nationality,
            email = excluded.email,
            birth_date = excluded.birth_date,
            signup_date = excluded.signup_date,
            source_system = excluded.source_system,
            is_active = true,
            deleted_date = NULL,
            update_timestamp = now()
    """)

    con.execute(
        """
        UPDATE silver_passengers
        SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true
          AND passenger_id NOT IN (SELECT passenger_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.execute("DROP TABLE snapshot")
    con.unregister("snapshot_en")
    con.unregister("snapshot_fr")
    con.close()


def ingest_gold():
    """Reconstruit dim_passenger à partir de l'état courant de silver_passengers.

    On ajoute :
      - age : l'âge exact aujourd'hui. age(current_date, birth_date) donne un intervalle
        (ex. "34 years 2 months 5 days"), on n'en garde que les années. On n'utilise pas une
        simple différence d'années (2026 - 1991), qui serait fausse avant l'anniversaire ;
      - age_range : la tranche d'âge, calculée à partir de cet âge
    """
    con = get_connection()
    con.execute("""
        CREATE OR REPLACE TABLE dim_passenger AS
        SELECT
            *,
            CASE
                WHEN age < 18 THEN '0-17'
                WHEN age < 25 THEN '18-24'
                WHEN age < 35 THEN '25-34'
                WHEN age < 45 THEN '35-44'
                WHEN age < 55 THEN '45-54'
                WHEN age < 65 THEN '55-64'
                ELSE '65+'
            END AS age_range
        FROM (
            SELECT *, date_part('year', age(current_date, birth_date)) AS age
            FROM silver_passengers
        )
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Passagers (init) ingérés.")


if __name__ == "__main__":
    init()
