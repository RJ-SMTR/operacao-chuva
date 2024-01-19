from datetime import datetime
from typing import Union

import folium
import requests

import streamlit as st
from streamlit_folium import st_folium

from zipfile import ZipFile
import pandas as pd
import geopandas as gpd
from folium.plugins import HeatMap
from shapely.geometry import LineString

import basedosdados as bd
bd.config.billing_project_id = "rj-smtr-dev"

st.set_page_config(layout="wide", page_title="Monitoramento do sistema de transportes do Rio")

# st.image("./data/logo/logo.png", width=300)

def main():

    st.markdown("# Monitoramento de alagamentos no sistema de transporte municipal")
    st.markdown(
        """Uma descrição aqui"""
    )

    m = folium.Map(location=[-22.917690, -43.413861], zoom_start=11)

    try:
        raw_api_data = requests.get(
            "https://api.dados.rio/v2/clima_alagamento/alagamento_detectado_ia/"
        ).json()
        last_update = requests.get(
            "https://api.dados.rio/v2/clima_alagamento/ultima_atualizacao_alagamento_detectado_ia/"
        ).text.strip('"')
        st.session_state["error"] = False
    except Exception as exc:
        raw_api_data = []
        last_update = ""
        st.session_state["error"] = True
    
    # Status Dados
    st.markdown(
        f"""
        {"✅" if not st.session_state["error"] else "❌"} **API Alagamentos**: {datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")}
    """,
    )

    # Mapa
    chart_data = pd.DataFrame(raw_api_data)

    ## Camada Alertas
    def get_icon_color(label: Union[bool, None]):
        if label:
            return "orange"
        else:
            return "gray"
        # elif label is None:
        #     return "gray"
        # else:
        #     return "green"

    for i in range(0, len(chart_data)):
        ai_classification = chart_data.iloc[i].get("ai_classification", [])
        if ai_classification == []:
            ai_classification = [{"label": None}]
        folium.Marker(
            location=[chart_data.iloc[i]["latitude"], chart_data.iloc[i]["longitude"]],
            # add nome_da_camera and status to tooltip
            tooltip=f"""
            ID: {chart_data.iloc[i]['id_camera']}""",
            # change icon color according to status
            icon=folium.Icon(color=get_icon_color(ai_classification[0].get("label", None))),
            # icon=folium.CustomIcon(
            #     icon_data["url"],
            #     icon_size=(icon_data["width"], icon_data["height"]),
            #     icon_anchor=(icon_data["width"] / 2, icon_data["height"]),
            # ),
        ).add_to(m)

    ## Camada Níveis de Alagamento

    with ZipFile("../src/data/gtfs_2024-01-15_2024-01-31.zip") as myzip:

        # TODO: add filters by servico & consorcio
        
        # agency = pd.read_csv(
        #     myzip.open("shapes.txt"), 
        #     dtype={
        #         'trip_id': 'str', 
        #         'trip_short_name': 'str', 
        #         'trip_long_name': 'str',  
        #         'shape_pt_sequence': 'Int64', 
        #         'shape_dist_traveled': 'float',
        #     })

        # routes = pd.read_csv(
        #     myzip.open("shapes.txt"), 
        #     dtype={
        #         'trip_id': 'str', 
        #         'trip_short_name': 'str', 
        #         'trip_long_name': 'str',  
        #         'shape_pt_sequence': 'Int64', 
        #         'shape_dist_traveled': 'float',
        #     })
        
        # trips = pd.read_csv(
        #     myzip.open("shapes.txt"), 
        #     dtype={
        #         'trip_id': 'str', 
        #         'trip_short_name': 'str', 
        #         'trip_long_name': 'str',  
        #         'shape_pt_sequence': 'Int64', 
        #         'shape_dist_traveled': 'float',
        #     })

        shapes = pd.read_csv(
            myzip.open("shapes.txt"), 
            dtype={
                'shape_id': 'str', 
                'shape_pt_lat': 'float', 
                'shape_pt_lon': 'float',  
                'shape_pt_sequence': 'Int64', 
                'shape_dist_traveled': 'float',
            })

        shapes = gpd.GeoDataFrame(shapes,
            geometry=gpd.points_from_xy(shapes.shape_pt_lon, shapes.shape_pt_lat)
        ).set_crs(epsg=4326)

    # Sort shapes by shape_pt_sequence
    shapes.sort_values(['shape_id','shape_pt_sequence'], inplace=True)
    shapes = (
        shapes[["shape_id", "shape_pt_lat", "shape_pt_lon"]]
        .groupby("shape_id")
        .agg(list)
        .apply(lambda x: LineString(zip(x[1], x[0])), axis=1)
    )

    shapes = gpd.GeoDataFrame(
        data=shapes.index,
        geometry = shapes.values,
        crs=4326
    )
    shapes['shape_id'] = shapes.shape_id.astype(str)

    ocorrencias = pd.read_csv("../src/data/ocorrencias_alagamento_2022-01-01_2024-01-18.csv")

    t = ocorrencias.groupby(["latitude", "longitude"]).gravidade.count().reset_index()

    # m = folium.Map(location=[-22.917690, -43.413861], zoom_start=11)

    HeatMap(t, min_opacity=0.4, blur = 18).add_to(folium.FeatureGroup(name='Heat Map').add_to(m))
    folium.GeoJson(shapes['geometry'], color='blue', weight=2.5, opacity=1).add_to(m)

    folium.TileLayer('cartodbpositron').add_to(m)
    folium.LayerControl().add_to(m)

    map_data = st_folium(m, key="mapa", height=600, width=1200)

    # Tabela
    # q = """
    # WITH
    # r AS(
    # SELECT
    #     DISTINCT
    #     CASE
    #     WHEN route_type = "200" OR route_type = "700" THEN "Ônibus"
    #     WHEN route_type = "702" THEN "BRT"
    # END
    #     AS modo,
    #     agency_id,
    #     route_short_name
    # FROM
    #     `rj-smtr.br_rj_riodejaneiro_gtfs.routes`
    # WHERE
    #     data_versao = "2023-12-21" ),
    # a AS (
    # SELECT
    #     agency_id,
    #     agency_name
    # FROM
    #     `rj-smtr.br_rj_riodejaneiro_gtfs.agency`
    # WHERE
    #     data_versao = "2023-12-21" ),
    # g AS (
    # SELECT
    #     route_short_name,
    #     COUNT(ordem) AS quantidade_veiculos_operacao,
    #     SUM(CASE
    #         WHEN velocidade_media < 3 THEN 1
    #     ELSE
    #     0
    #     END
    #     ) AS quantidade_veiculos_parados
    # FROM (
    #     SELECT
    #     CONCAT( IFNULL(REGEXP_EXTRACT(linha, r'[A-Z]+'), ""), IFNULL(REGEXP_EXTRACT(linha, r'[0-9]+'), "") ) AS route_short_name,
    #     ordem,
    #     AVG(velocidade) AS velocidade_media
    #     FROM
    #     `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros`
    #     WHERE
    #     DATA = CURRENT_DATE("America/Sao_Paulo")
    #     AND timestamp_gps >= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 10 minute)
    #     GROUP BY
    #     1,
    #     2 )
    # GROUP BY
    #     1),
    # gh AS (
    # SELECT
    #     route_short_name,
    #     AVG(id_veiculo) AS quantidade_veiculos_operacao_historico
    # FROM (
    #     SELECT
    #     DATA,
    #     servico AS route_short_name,
    #     COUNT(DISTINCT id_veiculo) AS id_veiculo,
    #     FROM
    #     `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    #     WHERE
    #     DATA >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 1 day)
    #     AND EXTRACT(time FROM timestamp_gps) BETWEEN
    #         EXTRACT(time FROM DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 10 minute))
    #         AND EXTRACT(time FROM CURRENT_DATETIME("America/Sao_Paulo"))
    #     GROUP BY
    #     1,
    #     2)
    # GROUP BY
    #     1)
    # SELECT
    # modo,
    # agency_name,
    # route_short_name,
    # IFNULL(quantidade_veiculos_parados, 0 ) AS quantidade_veiculos_parados,
    # IFNULL(quantidade_veiculos_operacao, 0) AS quantidade_veiculos_operacao,
    # IFNULL(quantidade_veiculos_operacao_historico, 0) AS quantidade_veiculos_operacao_historico,
    # CURRENT_DATETIME("America/Sao_Paulo") AS ultima_atualizacao
    # FROM
    # r
    # LEFT JOIN
    # a
    # USING
    # (agency_id)
    # LEFT JOIN
    # g
    # USING
    # (route_short_name)
    # LEFT JOIN
    # gh
    # USING
    # (route_short_name)
    # """

    # df = bd.read_sql(q)
    # st.table(df)

main()