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

    load_history(con, snapshot_date)  # bonus SCD2, à partir du même snapshot

    con.unregister("snapshot")
    con.close()


# ---------------------------------------------------------------------------------------------
# Bonus SCD2 : historique complet des vols.
#
# silver_flights (au-dessus) ne garde que la DERNIÈRE version de chaque vol (SCD1 : on écrase).
# silver_flights_history garde TOUTES les versions, chacune avec sa période de validité :
#   valid_from = 1er jour où cette version est vue
#   valid_to   = jour où elle a été remplacée ou a disparu (NULL = version encore en cours)
# Une version est donc valable du valid_from (inclus) au valid_to (exclu).
#
# Exemple : F0042 change d'avion le 2025-09-10
#   F0042 | Airbus A320 | valid_from 2025-08-31 | valid_to 2025-09-10 | is_current false
#   F0042 | Boeing 737  | valid_from 2025-09-10 | valid_to NULL       | is_current true

def create_history_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights_history (
            flight_id VARCHAR,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP,
            PRIMARY KEY (flight_id, valid_from)  -- un vol a au plus une version par jour
        )
    """)


def load_history(con, snapshot_date: date):
    """Met à jour silver_flights_history à partir du snapshot du jour (table "snapshot")."""
    create_history_table(con)

    con.execute("DELETE FROM silver_flights_history WHERE valid_from >= ?", [snapshot_date])
    con.execute(
        """
        UPDATE silver_flights_history SET valid_to = NULL, is_current = true
        WHERE valid_to >= ?
        """,
        [snapshot_date],
    )

    con.execute("""
        CREATE OR REPLACE TEMP TABLE snapshot_typed AS
        SELECT
            flight_id, flight_number, airline, origin_airport_id, destination_airport_id,
            CAST(flight_date AS DATE) AS flight_date,
            CAST(departure_time AS TIME) AS departure_time,
            CAST(arrival_time AS TIME) AS arrival_time,
            aircraft_type
        FROM snapshot
    """)

    con.execute(
        """
        UPDATE silver_flights_history AS h
        SET valid_to = ?, is_current = false, update_timestamp = now()
        WHERE h.is_current = true
          AND NOT EXISTS (
              SELECT 1 FROM snapshot_typed AS s
              WHERE s.flight_id = h.flight_id
                AND s.flight_number IS NOT DISTINCT FROM h.flight_number
                AND s.airline IS NOT DISTINCT FROM h.airline
                AND s.origin_airport_id IS NOT DISTINCT FROM h.origin_airport_id
                AND s.destination_airport_id IS NOT DISTINCT FROM h.destination_airport_id
                AND s.flight_date IS NOT DISTINCT FROM h.flight_date
                AND s.departure_time IS NOT DISTINCT FROM h.departure_time
                AND s.arrival_time IS NOT DISTINCT FROM h.arrival_time
                AND s.aircraft_type IS NOT DISTINCT FROM h.aircraft_type
          )
        """,
        [snapshot_date],
    )

    con.execute(
        """
        INSERT INTO silver_flights_history
        SELECT s.*, ?, NULL, true, now(), now()   -- valid_from, valid_to, is_current, timestamps
        FROM snapshot_typed AS s
        WHERE s.flight_id NOT IN (
            SELECT flight_id FROM silver_flights_history WHERE is_current = true
        )
        """,
        [snapshot_date],
    )

    con.execute("DROP TABLE snapshot_typed")


def ingest_gold():
    """Reconstruit dim_flight à partir de l'état courant de silver_flights.

    On ajoute la durée de vol calculée, en minutes :
      - cas normal : arrivée - départ (ex. 10:30 -> 12:02 = 92 min) ;
      - vol qui arrive le lendemain (arrivée < départ, ex. 15:30 -> 03:41) : la différence
        est négative, on ajoute 24 h = 1440 min pour obtenir la vraie durée (731 min).
    """
    con = get_connection()
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_flight AS
        SELECT *, {DURATION_SQL} AS flight_duration_minutes
        FROM silver_flights
    """)
    # Bonus SCD2
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_flight_history AS
        SELECT *, {DURATION_SQL} AS flight_duration_minutes
        FROM silver_flights_history
    """)
    con.close()


# Formule de durée partagée par dim_flight et dim_flight_history (écrite une seule fois).
DURATION_SQL = """
    CASE
        WHEN arrival_time >= departure_time
            THEN date_diff('minute', departure_time, arrival_time)
        ELSE date_diff('minute', departure_time, arrival_time) + 1440
    END
"""


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Vols (init) ingérés.")


if __name__ == "__main__":
    init()
