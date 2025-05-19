
from __future__ import annotations

import matplotlib.pyplot as plt
import streamlit as st

from backend import (
    get_kpi_material,
    get_kpi_people,
    get_mission_stats,
)


@st.cache_data(ttl=3600)
def _people_df(office):
    return get_kpi_people(office)


@st.cache_data(ttl=3600)
def _mat_df(office):
    return get_kpi_material(office)


@st.cache_data(ttl=3600)
def _missions_data(office):
    return get_mission_stats(office)


def display_people_kpi(office: str):
    df = _people_df(office)
    if office == "ALL_OFFICES":
        df = df.sort_values("BGES Total (t CO₂e)", ascending=False)
    st.subheader("👥 Personnel")
    st.dataframe(df, hide_index=True, use_container_width=True)


def display_material_kpi(office: str):
    df = _mat_df(office)
    st.subheader("💻 Matériel informatique")
    st.dataframe(df, hide_index=True, use_container_width=True)


def display_missions(office: str):
    mens_df, top_dest = _missions_data(office)

    st.subheader("✈️ Missions mensuelles")
    fig, ax = plt.subplots(figsize=(8, 3))
    ax.plot(mens_df["MOIS"], mens_df["nb_missions"], marker="o")
    ax.set_xlabel("Mois")
    ax.set_ylabel("Nombre de missions")
    ax.tick_params(axis="x", rotation=45)
    st.pyplot(fig, clear_figure=True)

    st.subheader("🏆 TOP 5 destinations")
    st.dataframe(top_dest, hide_index=True, use_container_width=True)
