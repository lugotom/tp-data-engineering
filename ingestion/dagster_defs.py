"""
Bonus : orchestration du pipeline avec Dagster (remplace run_month.py).

Les fonctions ingest_bronze / ingest_silver / ingest_gold des scripts d'ingestion ne changent
pas : chaque fonction est simplement "emballée" dans un asset Dagster (une fonction ≈ un asset).

Graphe des assets :

    init_load (état de départ : dim_currency + fichiers init/ des 4 entités)
        │
    bronze_<entité>  ──►  silver_<entité>       partitionnés par jour (1 → 30 septembre)
                               │
                          dim_airport, dim_flight, dim_passenger, fact_booking
                               │                                  (non partitionnés)
                          agg_revenue_daily_airline

Lancer (depuis la racine du repo, venv activé) :
    pip install -r requirements-dagster.txt
    dagster dev -f ingestion/dagster_defs.py -d ingestion   (-d : dossier où trouver common.py, etc.)
puis ouvrir http://localhost:3000 (marche à suivre dans DOCUMENTATION.md).
"""
from datetime import date

import dagster as dg

import build_aggregates
import ingest_airports
import ingest_bookings
import ingest_flights
import ingest_passengers
import run_month

# Une partition = un jour de septembre 2025 (end_date est exclue).
daily = dg.DailyPartitionsDefinition(start_date="2025-09-01", end_date="2025-10-01")

# Un backfill de 30 jours s'exécute en UN SEUL run qui reçoit toute la plage de dates, au lieu
# de 30 runs lancés en parallèle. Deux raisons :
#   - le silver doit traiter les jours DANS L'ORDRE (on compare le snapshot du jour à l'état de la
#     veille : désactivations, historique SCD2) ;
#   - DuckDB n'accepte qu'un seul processus en écriture à la fois sur warehouse.duckdb.
one_run = dg.BackfillPolicy.single_run()

ENTITIES = {
    "airports": ingest_airports,
    "flights": ingest_flights,
    "passengers": ingest_passengers,
    "bookings": ingest_bookings,
}


@dg.asset(group_name="init")
def init_load() -> None:
    """État de départ (avant septembre) : table dim_currency + fichiers init/ des 4 entités."""
    run_month.init_dim_currency()
    for module in ENTITIES.values():
        module.ingest_bronze(init=True)
    for module in ENTITIES.values():
        module.ingest_silver(init=True)


def make_daily_assets(name, module):
    """Crée les 2 assets partitionnés (bronze + silver) d'une entité.

    Écrit une fois et appelé pour chaque entité, plutôt que de recopier 8 fois le même code.
    """

    @dg.asset(name=f"bronze_{name}", group_name="bronze",
              partitions_def=daily, backfill_policy=one_run)
    def bronze(context: dg.AssetExecutionContext) -> None:
        # context.partition_keys = les jours à traiter dans ce run, ex. ["2025-09-01", ...]
        for key in context.partition_keys:
            module.ingest_bronze(date.fromisoformat(key))

    @dg.asset(name=f"silver_{name}", group_name="silver",
              partitions_def=daily, backfill_policy=one_run,
              deps=[bronze, init_load])
    def silver(context: dg.AssetExecutionContext) -> None:
        for key in sorted(context.partition_keys):  # sorted : toujours dans l'ordre des jours
            module.ingest_silver(date.fromisoformat(key))

    return bronze, silver


daily_assets = []
for name, module in ENTITIES.items():
    daily_assets += make_daily_assets(name, module)


# Gold : non partitionné, car il ne dépend que de l'état COURANT du silver (pas d'une date).
@dg.asset(group_name="gold", deps=["silver_airports"])
def dim_airport() -> None:
    ingest_airports.ingest_gold()


@dg.asset(group_name="gold", deps=["silver_flights"])
def dim_flight() -> None:
    """Construit dim_flight et dim_flight_history (bonus SCD2)."""
    ingest_flights.ingest_gold()


@dg.asset(group_name="gold", deps=["silver_passengers"])
def dim_passenger() -> None:
    ingest_passengers.ingest_gold()


@dg.asset(group_name="gold", deps=["silver_bookings", init_load])  # init_load : dim_currency
def fact_booking() -> None:
    ingest_bookings.ingest_gold()


@dg.asset(group_name="gold", deps=[fact_booking, dim_flight])
def agg_revenue_daily_airline() -> None:
    build_aggregates.build_agg_revenue_daily_airline()


defs = dg.Definitions(
    assets=[init_load, *daily_assets,
            dim_airport, dim_flight, dim_passenger, fact_booking, agg_revenue_daily_airline],
    # Tous les assets d'un run s'exécutent l'un après l'autre dans le même processus
    # (par défaut Dagster les lance en parallèle, ce que DuckDB ne supporte pas en écriture).
    executor=dg.in_process_executor,
)
