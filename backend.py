"""
backend.py – Spark session, vues CSV, KPI et requêtes “interrogation”.
BGES en tonne de CO₂e.
"""
from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path
from typing import Iterable, List

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

# --------------------------------------------------------------------------- #
# PARAMÈTRES GLOBAUX                                                          #
# --------------------------------------------------------------------------- #
_DATA_PATH = Path(__file__).resolve().parent.parent / "csv_files"
_DATA_PATH = Path(os.getenv("BGES_DATA_PATH", _DATA_PATH))

OFFICES = ["LOSANGELES", "PARIS", "LONDON", "NEWYORK", "SHANGHAI", "BERLIN"]
JOBS = ["Business Executive", "Data Engineer", "Economist", "HRD"]
MAT_TYPES = [
    "PC fixe sans ecran",
    "PC portable",
    "Serveur",
    "PC fixe tout-en-un",
    "Ecran",
]
MISSION_TYPES = [
    "Vocational Training",
    "Conference",
    "Business Meeting",
    "Development",
    "Team Meeting",
]

_spark: SparkSession | None = None
_views_loaded = False

# --------------------------------------------------------------------------- #
# SPARK                                                                       #
# --------------------------------------------------------------------------- #
def get_spark() -> SparkSession:
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder.master("local[*]")
            .appName("bges_star_schema")
            .getOrCreate()
        )
        _spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def load_views(force: bool = False) -> None:
    global _views_loaded
    if _views_loaded and not force:
        return

    spark = get_spark()
    for name in ["mission", "materiel", "personnel", "date", "emission_fact"]:
        df = (
            spark.read.option("header", True)
            .option("sep", ";")
            .option("encoding", "utf-8")
            .csv(str(_DATA_PATH / f"dim_{name}"))
            .cache()
        )
        if name == "emission_fact":
            df = df.withColumn("BGES_DBL", F.col("BGES").cast("double"))
        df.createOrReplaceTempView(name)

    _views_loaded = True


def _ensure_views() -> None:
    if not _views_loaded:
        load_views()


# --------------------------------------------------------------------------- #
# KPI TABLEAU DE BORD                                                         #
# --------------------------------------------------------------------------- #
def get_kpi_people(office: str = "ALL_OFFICES"):
    _ensure_views()
    spark = get_spark()

    pers = spark.table("personnel").filter(F.col("Office_localisation").isin(OFFICES))
    ef = spark.table("emission_fact")

    eff = (
        pers.groupBy("Office_localisation")
        .agg(F.countDistinct("ID_PERSONNEL").alias("Employés"))
    )

    bges_mis = (
        ef.filter("type_emission = 'mission'")
        .join(F.broadcast(pers), "ID_PERSONNEL")
        .groupBy("Office_localisation")
        .agg((F.sum("BGES_DBL") / 1000).alias("BGES Missions (t CO₂e)"))
    )

    bges_mat = (
        ef.filter("type_emission = 'materiel_informatique'")
        .join(F.broadcast(pers), "ID_PERSONNEL")
        .groupBy("Office_localisation")
        .agg((F.sum("BGES_DBL") / 1000).alias("BGES Matériel (t CO₂e)"))
    )

    kpi = (
        eff.join(bges_mis, "Office_localisation", "left")
        .join(bges_mat, "Office_localisation", "left")
        .fillna(0.0)
        .withColumn(
            "BGES Total (t CO₂e)",
            F.col("BGES Missions (t CO₂e)") + F.col("BGES Matériel (t CO₂e)"),
        )
        .withColumnRenamed("Office_localisation", "Office")
    )

    if office != "ALL_OFFICES":
        kpi = kpi.filter(F.col("Office") == office)

    return kpi.toPandas()


def get_kpi_material(office: str = "ALL_OFFICES"):
    """
    • Matériel le plus acheté (type + nb d’achats)
    • Matériel le plus polluant (type uniquement)
    """
    _ensure_views()
    spark = get_spark()

    pers = spark.table("personnel").filter(F.col("Office_localisation").isin(OFFICES))
    ef_mat = (
        spark.table("emission_fact")
        .filter("type_emission = 'materiel_informatique'")
        .join(F.broadcast(pers), "ID_PERSONNEL")
        .alias("ef")
    )
    mat = spark.table("materiel").alias("mat")

    joined = (
        ef_mat.join(mat, F.col("ef.ID_EMISSION") == F.col("mat.ID_MATERIELINFO"), "left")
        .select(
            F.col("ef.Office_localisation").alias("Office"),
            F.col("mat.TYPE").alias("Type_materiel"),
            F.col("ef.BGES_DBL"),
        )
    )

    stats = (
        joined.groupBy("Office", "Type_materiel")
        .agg(
            F.count("*").alias("Nombre d’achats"),
            F.sum("BGES_DBL").alias("BGES_sum"),  # kg
        )
    )

    # plus acheté
    top_buy = (
        stats.withColumn(
            "rk_buy",
            F.row_number().over(
                Window.partitionBy("Office").orderBy(F.desc("Nombre d’achats"))
            ),
        )
        .filter("rk_buy = 1")
        .select(
            "Office",
            F.col("Type_materiel").alias("Matériel le plus acheté"),
            "Nombre d’achats",
        )
    )

    # plus polluant : on ne garde que le type_materiel
    top_bges = (
        stats.withColumn(
            "rk_bges",
            F.row_number().over(
                Window.partitionBy("Office").orderBy(F.desc("BGES_sum"))
            ),
        )
        .filter("rk_bges = 1")
        .select(
            "Office",
            F.col("Type_materiel").alias("Matériel le plus polluant"),
        )
    )

    kpi = top_buy.join(top_bges, "Office", "outer")

    if office != "ALL_OFFICES":
        kpi = kpi.filter(F.col("Office") == office)

    return kpi.toPandas()


def get_mission_stats(office: str = "ALL_OFFICES"):
    """
    Nb missions par mois + TOP 5 destinations.
    """
    _ensure_views()
    spark = get_spark()
    pers = spark.table("personnel").filter(F.col("Office_localisation").isin(OFFICES))

    ef_mis = (
        spark.table("emission_fact")
        .filter("type_emission = 'mission'")
        .join(spark.table("mission"), F.col("ID_EMISSION") == F.col("ID_MISSION"))
        .join(F.broadcast(pers), "ID_PERSONNEL")
        .join(spark.table("date"), "DATE_ID")
        .withColumn("MOIS", F.date_format("DATE_EMISSION", "yyyy-MM"))
    )

    if office != "ALL_OFFICES":
        ef_mis = ef_mis.filter(F.col("Office_localisation") == office)

    mens = ef_mis.groupBy("MOIS").agg(F.count("*").alias("nb_missions"))
    df_mens = mens.orderBy("MOIS").toPandas()

    top_dest = (
        ef_mis.groupBy("VILLE_DESTINATION")
        .agg(F.count("*").alias("Missions"))
        .orderBy(F.desc("Missions"))
        .limit(5)
        .withColumnRenamed("VILLE_DESTINATION", "Destination")
        .toPandas()
    )

    return df_mens, top_dest



# --------------------------------------------------------------------------- #
# 5. OUTILS GÉNÉRIQUES                                                        #
# --------------------------------------------------------------------------- #
def _date_filter(start: _dt.date, end: _dt.date):
    spark = get_spark()
    return spark.table("date").filter(
        (F.col("DATE_EMISSION") >= F.lit(start.isoformat()))
        & (F.col("DATE_EMISSION") <= F.lit(end.isoformat()))
    )


def _norm_list(values: Iterable[str], reference: List[str]) -> List[str]:
    normalized = [v for v in values if v in reference]
    return reference if not normalized else normalized


# --------------------------------------------------------------------------- #
# 6. ANALYSE INTERROGATION                                                    #
# --------------------------------------------------------------------------- #
def carbon_impact_material(
    material_type: str,
    jobs: List[str],
    offices: List[str],
    start: _dt.date,
    end: _dt.date,
) -> float:
    """
    Impact carbone (t CO₂e) d’un matériel, métiers/offices/période.
    """
    _ensure_views()
    spark = get_spark()

    offices = _norm_list([o.upper() for o in offices], OFFICES)
    jobs = _norm_list(jobs, JOBS)

    pers = (
        spark.table("personnel")
        .filter(F.col("Office_localisation").isin(offices))
        .filter(F.col("FONCTION_PERSONNEL").isin(jobs))
        .alias("pers")
    )

    mat = (
        spark.table("materiel")
        .filter(F.col("TYPE") == material_type)
        .select(F.col("ID_MATERIELINFO").alias("ID_EMISSION"))
        .alias("mat")
    )

    date_df = _date_filter(start, end).alias("d")

    impact = (
        spark.table("emission_fact")
        .filter("type_emission = 'materiel_informatique'")
        .alias("ef")
        .join(mat, "ID_EMISSION")
        .join(F.broadcast(pers), "ID_PERSONNEL")
        .join(date_df, "DATE_ID")
        .agg((F.sum("BGES_DBL") / 1000).alias("impact"))
        .collect()[0]["impact"]
    )

    return impact or 0.0


def carbon_impact_mission(
    mission_types: List[str],
    jobs: List[str],
    offices: List[str],
    start: _dt.date,
    end: _dt.date,
) -> float:
    """
    Impact carbone (t CO₂e) d’un ou plusieurs types de missions.
    """
    _ensure_views()
    spark = get_spark()

    offices = _norm_list([o.upper() for o in offices], OFFICES)
    jobs = _norm_list(jobs, JOBS)
    mission_types = _norm_list(mission_types, MISSION_TYPES)

    pers = (
        spark.table("personnel")
        .filter(F.col("Office_localisation").isin(offices))
        .filter(F.col("FONCTION_PERSONNEL").isin(jobs))
        .alias("pers")
    )

    missions = (
        spark.table("mission")
        .filter(F.col("TYPE_MISSION").isin(mission_types))
        .select(F.col("ID_MISSION").alias("ID_EMISSION"))
        .alias("mis")
    )

    date_df = _date_filter(start, end).alias("d")

    impact = (
        spark.table("emission_fact")
        .filter("type_emission = 'mission'")
        .alias("ef")
        .join(missions, "ID_EMISSION")
        .join(F.broadcast(pers), "ID_PERSONNEL")
        .join(date_df, "DATE_ID")
        .agg((F.sum("BGES_DBL") / 1000).alias("impact"))
        .collect()[0]["impact"]
    )

    return impact or 0.0


def top_mission_categories(
    jobs: List[str],
    offices: List[str],
    start: _dt.date,
    end: _dt.date,
    top_n: int = 3,
):
    """
    TOP N catégories de missions les plus émettrices (t CO₂e).
    """
    _ensure_views()
    spark = get_spark()

    offices = _norm_list([o.upper() for o in offices], OFFICES)
    jobs = _norm_list(jobs, JOBS)

    pers = (
        spark.table("personnel")
        .filter(F.col("Office_localisation").isin(offices))
        .filter(F.col("FONCTION_PERSONNEL").isin(jobs))
        .alias("pers")
    )

    date_df = _date_filter(start, end).alias("d")

    df = (
        spark.table("emission_fact")
        .filter("type_emission = 'mission'")
        .join(F.broadcast(pers), "ID_PERSONNEL")
        .join(spark.table("mission"), F.col("ID_EMISSION") == F.col("ID_MISSION"))
        .join(date_df, "DATE_ID")
        .groupBy("TYPE_MISSION")
        .agg((F.sum("BGES_DBL") / 1000).alias("BGES_total (t CO₂e)"))
        .orderBy(F.desc("BGES_total (t CO₂e)"))
        .limit(top_n)
        .toPandas()
    )
    return df


def avg_age_training(
    mission_types: List[str],
    start: _dt.date,
    end: _dt.date,
) -> float:
    """
    Âge moyen (en années, deux décimales) des employés partis
    en mission de type `mission_types` sur la période.
    """
    _ensure_views()
    spark = get_spark()

    mission_types = _norm_list(mission_types, MISSION_TYPES)

    missions = (
        spark.table("mission")
        .filter(F.col("TYPE_MISSION").isin(mission_types))
        .select(F.col("ID_MISSION").alias("ID_EMISSION"))
        .alias("mis")
    )

    date_df = _date_filter(start, end).alias("d")

    data = (
        spark.table("emission_fact")
        .filter("type_emission = 'mission'")
        .alias("ef")
        .join(missions, "ID_EMISSION")
        .join(date_df, "DATE_ID")
        .join(
            spark.table("personnel").select("ID_PERSONNEL", "DT_NAISS"),
            "ID_PERSONNEL",
        )
        .withColumn(
            "AGE",
            F.floor(F.datediff(F.col("DATE_EMISSION"), F.col("DT_NAISS")) / 365.25),
        )
        .agg(F.avg("AGE").alias("avg_age"))
        .collect()[0]["avg_age"]
    )

    return round(float(data), 2) if data is not None else 0.0


def rank_offices_bges(
    start: _dt.date,
    end: _dt.date,
    emission_type: str = "all",
    top_n: int | None = None,
):
    """
    Classe les Offices par BGES sur la période donnée.
    `emission_type` : "mission", "materiel" ou "all"
    Renvoie un DataFrame pandas.
    """
    _ensure_views()
    spark = get_spark()

    ef = spark.table("emission_fact")
    if emission_type == "mission":
        ef = ef.filter("type_emission = 'mission'")
    elif emission_type == "materiel":
        ef = ef.filter("type_emission = 'materiel_informatique'")

    date_df = _date_filter(start, end).alias("d")

    ranking = (
        ef.join(spark.table("personnel"), "ID_PERSONNEL")
        .join(date_df, "DATE_ID")
        .groupBy("Office_localisation")
        .agg((F.sum("BGES_DBL") / 1000).alias("BGES Total (t CO₂e)"))
        .orderBy(F.desc("BGES Total (t CO₂e)"))
        .withColumnRenamed("Office_localisation", "Office")
    )

    if top_n:
        ranking = ranking.limit(top_n)

    return ranking.toPandas()
