"""
Tables d'agrégation de la couche gold.

Étape à part, lancée UNE fois après la boucle quotidienne de run_month.py (et pas dans
l'ingest_gold d'une entité) car :
  - elle dépend de plusieurs tables gold à la fois (fact_booking ET dim_flight)
  - elle se reconstruit entièrement à partir du gold : inutile de la recalculer chaque jour.
"""
from common import get_connection


def build_agg_revenue_daily_airline():
    """Chiffre d'affaires (en EUR) et nombre de réservations, par jour et par compagnie.

    - "par jour" = date de réservation (booking_date) : le CA est compté le jour où il est vendu ;
    - "par compagnie" = airline, qui n'est pas dans fact_booking : on la récupère dans dim_flight via flight_id.
    """
    con = get_connection()
    con.execute("""
        CREATE OR REPLACE TABLE agg_revenue_daily_airline AS
        SELECT
            b.booking_date,
            f.airline,
            COUNT(*) AS nb_bookings,
            SUM(b.amount_eur) AS revenue_eur,
            CAST(now() AS TIMESTAMP) AS insert_timestamp,  -- même type que les autres tables
            CAST(now() AS TIMESTAMP) AS update_timestamp
        FROM fact_booking AS b
        JOIN dim_flight AS f ON b.flight_id = f.flight_id
        GROUP BY b.booking_date, f.airline
        ORDER BY b.booking_date, f.airline
    """)
    con.close()


def run():
    build_agg_revenue_daily_airline()
    print("Agrégats gold construits.")


if __name__ == "__main__":
    run()
