
from __future__ import annotations

import datetime as dt
from pathlib import Path

import streamlit as st

import backend as bkd
import dashboard_stats as ds

st.set_page_config(page_title="BGES Dashboard", page_icon="🌍", layout="wide")
bkd.load_views()

IMG_DIR = Path(__file__).parent / "images"

LABELS = {
    "ALL_OFFICES": "🌐 Tous les offices",
    "LOSANGELES": "Los Angeles",
    "PARIS": "Paris",
    "LONDON": "London",
    "NEWYORK": "New York",
    "SHANGHAI": "Shanghai",
    "BERLIN": "Berlin",
}
IMAGES = {
    "ALL_OFFICES": "World.png",
    "LOSANGELES": "Los angeles.png",
    "PARIS": "paris.png",
    "LONDON": "london.png",
    "NEWYORK": "newyork.png",
    "SHANGHAI": "shanghai.png",
    "BERLIN": "berlin.png",
}

# --------------------------------------------------------------------------- #
# En-tête
col1, col2 = st.columns([1, 5], gap="medium")
with col1:
    st.image(str(IMG_DIR / "Logo_utc.jpg"), width=90)
with col2:
    st.markdown(
        """
        ## Projet BGES – UV NF26  
        Réalisé par **Fadi Shafo** & **Elias Belloumi**
        """
    )
st.markdown("---")

mode = st.sidebar.radio("Choisissez un mode", ["Mode tableau de bord", "Mode interrogation"])

# ----------------------------- TABLEAU DE BORD ----------------------------- #
if mode == "Mode tableau de bord":
    office = st.selectbox(
        "Sélectionnez un Office",
        options=list(LABELS.keys()),
        format_func=lambda k: LABELS[k],
    )
    st.markdown("---")

    img_path = IMG_DIR / IMAGES.get(office, "")
    if img_path.exists():
        st.image(str(img_path), width=220)

    ds.display_people_kpi(office)
    ds.display_material_kpi(office)
    ds.display_missions(office)

# ----------------------------- INTERROGATION ------------------------------- #
else:
    st.header("Mode interrogation")

    # Impact matériel --------------------------------------------------------
    with st.expander("Impact carbone – Matériel"):
        c1, c2 = st.columns(2)
        mat_type = c1.selectbox("Type de matériel", bkd.MAT_TYPES)
        jobs = c1.multiselect("Métiers", bkd.JOBS, default=["Data Engineer"])
        offices = c2.multiselect("Offices", list(LABELS.keys())[1:], default=["PARIS", "NEWYORK"])
        d1, d2 = c2.columns(2)
        start = d1.date_input("Date début", dt.date(2024, 5, 1))
        end = d2.date_input("Date fin", dt.date(2024, 9, 30))
        if st.button("Calculer", key="btn_mat"):
            impact = bkd.carbon_impact_material(mat_type, jobs, offices, start, end)
            st.success(f"Impact carbone : **{impact:,.2f} t CO₂e**")

    # Impact missions --------------------------------------------------------
    with st.expander("Impact carbone – Missions"):
        c1, c2 = st.columns(2)
        m_types = c1.multiselect("Type(s) de mission", bkd.MISSION_TYPES, default=["Vocational Training"])
        jobs_m = c1.multiselect("Métiers", bkd.JOBS, default=["Business Executive"])
        offices_m = c2.multiselect("Offices", list(LABELS.keys())[1:], default=["LOSANGELES"])
        d1, d2 = c2.columns(2)
        s_m = d1.date_input("Date début", dt.date(2024, 7, 1), key="start_m")
        e_m = d2.date_input("Date fin", dt.date(2024, 7, 31), key="end_m")
        if st.button("Calculer", key="btn_mission"):
            impact_m = bkd.carbon_impact_mission(m_types, jobs_m, offices_m, s_m, e_m)
            st.success(f"Impact carbone : **{impact_m:,.2f} t CO₂e**")

    # TOP catégories missions ------------------------------------------------
    with st.expander("TOP catégories de missions les plus impactantes"):
        job_top = st.selectbox("Métier", bkd.JOBS, index=0, key="job_top")
        offices_top = st.multiselect("Offices", list(LABELS.keys())[1:], default=["PARIS", "LONDON", "BERLIN"])
        date_top = st.date_input("Mois concerné", dt.date(2024, 6, 15))
        top_n = st.slider("Nombre de catégories", 1, 10, 3)
        if st.button("Afficher TOP", key="btn_top"):
            first_day = date_top.replace(day=1)
            last_day = (first_day.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
            top_df = bkd.top_mission_categories([job_top], offices_top, first_day, last_day, top_n)
            st.dataframe(top_df, hide_index=True, use_container_width=True)

    # Âge moyen formation ----------------------------------------------------
    with st.expander("Âge moyen des employés partis en mission"):
        m_sel = st.multiselect("Type(s) de formation", bkd.MISSION_TYPES, default=["Vocational Training"])
        c1, c2 = st.columns(2)
        start_f = c1.date_input("Date début", dt.date(2024, 7, 1), key="start_f")
        end_f = c2.date_input("Date fin", dt.date(2024, 9, 30), key="end_f")
        if st.button("Calculer âge moyen", key="btn_age"):
            age = bkd.avg_age_training(m_sel, start_f, end_f)
            st.success(f"Âge moyen : **{age:.2f} ans**")

    # Classement BGES Offices ------------------------------------------------
    with st.expander("Classement des Offices par BGES"):
        e_type = st.radio("Type d'émission", ["Toutes", "Missions", "Matériel"])
        d1, d2 = st.columns(2)
        start_r = d1.date_input("Date début", dt.date(2024, 1, 1), key="start_r")
        end_r = d2.date_input("Date fin",   dt.date(2024, 12, 31), key="end_r")
        if st.button("Afficher classement", key="btn_rank"):
            typemap = {"Toutes": "all", "Missions": "mission", "Matériel": "materiel"}
            df_rank = bkd.rank_offices_bges(start_r, end_r, emission_type=typemap[e_type])
            st.dataframe(df_rank, hide_index=True, use_container_width=True)

st.markdown("---")
st.caption("Version 2025-05-17")
