"""
À compléter : ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import fetch_csv, get_connection


def _daily_file(day: date = None, init: bool = False):
    if init:
        return "init", "bookings.csv"
    assert day is not None
    return "2025-09", f"bookings_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie les réservations du jour (ou de init/) dans bronze/, sans transformation."""
    fetch_csv(*_daily_file(day, init))


def create_silver_table(con):
    # Table de fait (évènements immuables) : pas de is_active ni de deleted_date.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_bookings (
            booking_id VARCHAR PRIMARY KEY,
            passenger_id VARCHAR,
            flight_id VARCHAR,
            airport_id VARCHAR,
            seat_class VARCHAR,
            amount DECIMAL(10, 2),
            currency VARCHAR,
            booking_date DATE,
            booking_channel VARCHAR,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit les réservations du jour depuis bronze/ et les ajoute à silver_bookings.

    Le fichier ne contient QUE les réservations du jour (pas un snapshot complet), et une
    réservation ne change jamais : on fait donc un simple insert, sans update ni désactivation.
    ON CONFLICT DO NOTHING rend l'insert idempotent : si on relance le même jour, les
    booking_id déjà présents sont ignorés au lieu de créer des doublons (ou une erreur).
    """
    df = fetch_csv(*_daily_file(day, init))

    con = get_connection()
    create_silver_table(con)
    con.register("daily", df)

    con.execute("""
        INSERT INTO silver_bookings
        SELECT
            booking_id,
            passenger_id,
            flight_id,
            airport_id,
            seat_class,
            CAST(amount AS DECIMAL(10, 2)),
            currency,
            CAST(booking_date AS DATE),
            booking_channel,
            now(),  -- insert_timestamp
            now()   -- update_timestamp
        FROM daily
        ON CONFLICT (booking_id) DO NOTHING
    """)

    con.unregister("daily")
    con.close()


def ingest_gold():
    """Reconstruit fact_booking (la table de fait) à partir de silver_bookings.

    Une ligne = une réservation. Elle contient :
      - les clés vers les dimensions : passenger_id -> dim_passenger, flight_id -> dim_flight,
        airport_id -> dim_airport, currency -> dim_currency ;
      - les mesures : amount (montant d'origine) et amount_eur (montant converti en euros).

    amount_eur = amount * rate_to_eur, avec le taux lu dans dim_currency.
    """
    con = get_connection()
    con.execute("""
        CREATE OR REPLACE TABLE fact_booking AS
        SELECT
            b.*,
            CAST(b.amount * c.rate_to_eur AS DECIMAL(12, 2)) AS amount_eur
        FROM silver_bookings AS b
        LEFT JOIN dim_currency AS c ON b.currency = c.currency
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
