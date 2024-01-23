import branca.colormap as cm
import folium
import folium.plugins as plugins
import geopandas as gpd
import pandas as pd
import streamlit as st
import os

from shapely.geometry import LineString
from streamlit_folium import folium_static
from zipfile import ZipFile
from redis_sr import RedisSR

def load_shapes():
    # Carrega dados de rotas (shapes)
    with ZipFile("src/data/gtfs_2024-01-15_2024-01-31.zip") as myzip:
    
        shapes = pd.read_csv(myzip.open("shapes.txt"), dtype={
                    'shape_id': 'str', 
                    'shape_pt_lat': 'float', 
                    'shape_pt_lon': 'float',  
                    'shape_pt_sequence': 'Int64', 
                    'shape_dist_traveled': 'float',
                })
    
        shapes = gpd.GeoDataFrame(shapes,
            geometry=gpd.points_from_xy(shapes.shape_pt_lon, shapes.shape_pt_lat)
        ).set_crs(epsg=4326)
    
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
    return shapes

def main():
    
    st.set_page_config(layout="wide", page_title="Monitoramento de chuvas no sistema de transportes")
    st.markdown("# Monitoramento de chuvas no sistema de transportes")
    st.markdown(
        """Abaixo você pode monitorar as rotas de ônibus,
        volume de precipitação e possíveis pontos de
        impacto na operação em áreas de aprox. 100m2.""")
        
    st.markdown("""
    > 📍 Ponteiros mostram o **Número de veículos parados ou fora de rota**

    > 🌧️ Escala de cores mostra a **Estimativa de volume de
    precipitação (mm) na última 1 hora** (segundo a estação mais próxima
    da área).
    """)

    redis = RedisSR.from_url(os.getenv('CACHE_OPERACAO_CHUVA'))
    df_geo = redis.get('data')

    # Instancia o mapa
    st.button("Atualizar dados")
    m = folium.Map(location=[-22.917690, -43.413861], zoom_start=11)
    

    # Adiciona dados de indice de chuva dos tiles
    colormap = cm.LinearColormap(
        ["green", "yellow", "red"], 
        vmin=0, 
        vmax=100,
        caption="Acumulado de chuva na última hora (mm)"
    )
    colormap.add_to(m)
    colorscale_dict = df_geo.set_index("tile_id")["acumulado_chuva_1_h"]
    

    popup = folium.GeoJsonPopup(
        fields=[
            "horario_leitura_estacao", 
            "acumulado_chuva_15_min", 
            "acumulado_chuva_1_h", 
            "servico",
            "indicador_veiculo_parado_10_min",
            "indicador_veiculo_parado_30_min",
            "indicador_veiculo_fora_rota_10_min",
            "indicador_veiculo_fora_rota_30_min",
            "tile_id"
        ],
        aliases=[
            "🌧️ Hora da leitura:",
            "🌧️ Acumulado 15min:",
            "🌧️ Acumulado 1h:",
            "🚍 Serviços em operação: ",
            "🛑 Veículos parados 10min:",
            "🛑 Veículos parados 30min:",
            "⤴️ Veículos desviados 10min:",
            "⤴️ Veículos desviados 30min:",
            "📍 ID do polígono: "
        ],
        localize=True,
        labels=True,
        # style="background-color: yellow;",
    )

    folium.GeoJson(
        df_geo, 
        style_function=lambda feature: {
            "fillColor": colormap(colorscale_dict[feature["properties"]["tile_id"]]),
            "color": "black",
            "weight": .2,
            "fillOpacity": 0.7,
        },
        color='acumulado_chuva_15_min', 
        weight=2.5, 
        opacity=1,
        popup=popup
    ).add_to(m)
    
    # Adiciona icones de qtd de veiculos parados/fora da rota    
    
    for i in range(0, len(df_geo)):
        if (df_geo.iloc[i].indicador_veiculo_parado_10_min > 0) and (df_geo.iloc[i].indicador_veiculo_fora_rota_10_min > 0):
            folium.Marker(
                location=[df_geo.iloc[i].geometry.centroid.y, df_geo.iloc[i].geometry.centroid.x], 
                icon=plugins.BeautifyIcon(
                     icon="arrow-down", 
                     icon_shape="marker",
                     number=(df_geo.iloc[i].indicador_veiculo_parado_10_min + df_geo.iloc[i].indicador_veiculo_fora_rota_10_min).astype(str),
                     border_color="#0381a1",
                     background_color="#0381a1"
                 )
            ).add_to(m)
            
        elif df_geo.iloc[i].indicador_veiculo_parado_10_min > 0:
            folium.Marker(
                location=[df_geo.iloc[i].geometry.centroid.y, df_geo.iloc[i].geometry.centroid.x], 
                icon=plugins.BeautifyIcon(
                     icon="arrow-down", 
                     icon_shape="marker",
                     number=df_geo.iloc[i].indicador_veiculo_parado_10_min.astype(str),
                     border_color="#5cdafa",
                     background_color="#5cdafa"
                 )
            ).add_to(m)
    
        elif df_geo.iloc[i].indicador_veiculo_fora_rota_10_min > 0:
            folium.Marker(
                location=[df_geo.iloc[i].geometry.centroid.y, df_geo.iloc[i].geometry.centroid.x], 
                icon=plugins.BeautifyIcon(
                     icon="arrow-down", 
                     icon_shape="marker",
                     number=df_geo.iloc[i].indicador_veiculo_fora_rota_10_min.astype(str),
                     border_color="#5cdafa",
                     background_color="#5cdafa"
                 )
            ).add_to(m)

    shapes = load_shapes()

    # Adiciona rotas ao mapa
    folium.GeoJson(shapes['geometry'], color='gray', weight=1.5, opacity=.8).add_to(m)

    # Ajusta camadas
    folium.TileLayer('cartodbpositron').add_to(m)
    folium.LayerControl().add_to(m)

    map_data = folium_static(m, height=600, width=1200)
    

if __name__ == '__main__':
  main()
