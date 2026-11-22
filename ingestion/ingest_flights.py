"""
À compléter : ingestion de l'entité `flights`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import fetch_csv, get_connection


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "flights.csv"
    assert day is not None
    return "2025-09", f"flights_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie le snapshot flights du jour (ou de init/) dans bronze/, sans transformation."""
    fetch_csv(*_snapshot_file(day, init))


def create_silver_table(con):
    # Mêmes colonnes techniques que silver_airports (is_active, deleted_date, timestamps).
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights (
            flight_id VARCHAR PRIMARY KEY,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le snapshot flights depuis bronze/ et l'upsert dans silver_flights.

    flights est un snapshot complet (comme airports) mais qui change souvent : des vols sont
    ajoutés, modifiés (horaire, avion, date) ou disparaissent. Donc :
      - upsert : nouveau flight_id -> INSERT, flight_id déjà connu -> UPDATE des valeurs ;
      - désactivation : un vol absent du snapshot du jour passe à is_active = false
    """
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)
    snapshot_date = date(2025, 8, 31) if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot", df)  # rend le DataFrame interrogeable en SQL sous le nom "snapshot"

    con.execute("""
        INSERT INTO silver_flights
        SELECT
            flight_id,
            flight_number,
            airline,
            origin_airport_id,
            destination_airport_id,
            CAST(flight_date AS DATE),
            CAST(departure_time AS TIME),
            CAST(arrival_time AS TIME),
            aircraft_type,
            true,   -- is_active
            NULL,   -- deleted_date
            now(),  -- insert_timestamp
            now()   -- update_timestamp
        FROM snapshot
        ON CONFLICT (flight_id) DO UPDATE SET
            flight_number = excluded.flight_number,
            airline = excluded.airline,
            origin_airport_id = excluded.origin_airport_id,
            destination_airport_id = excluded.destination_airport_id,
            flight_date = excluded.flight_date,
            departure_time = excluded.departure_time,
            arrival_time = excluded.arrival_time,
            aircraft_type = excluded.aircraft_type,
            is_active = true,
            deleted_date = NULL,
            update_timestamp = now()
    """)

    con.execute(
        """
        UPDATE silver_flights
        SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true
          AND flight_id NOT IN (SELECT flight_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.unregister("snapshot")
    con.close()


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données flights à partir de silver_flights
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Vols (init) ingérés.")


if __name__ == "__main__":
    init()
