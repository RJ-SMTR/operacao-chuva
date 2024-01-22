import basedosdados as bd
import branca.colormap as cm
from datetime import datetime, timedelta
import folium
import folium.plugins as plugins
import geopandas as gpd
import pandas as pd
from shapely.wkt import loads
from shapely.geometry import LineString
import streamlit as st
from streamlit_folium import st_folium
from zipfile import ZipFile

bd.config.billing_project_id = "rj-smtr-dev"

# st.image("./data/logo/logo.png", width=300)

@st.cache_data
def load_shapes():
    # print(">>> AQUI DENTRO SHAPES:", datetime.now())
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

@st.cache_data
def load_gps(datahora, data_versao_gtfs):
    # print(">>> AQUI DENTRO GPS:", datetime.now())
    # Carrega dados da operação
    q = f"""
    -- 1. Puxa dados de posição de GPS dos ônibus e flag indicativa de parada
    with gps AS (
      SELECT
        servico,
        id_veiculo,
        latitude,
        longitude,
        timestamp_gps,
        CASE
          WHEN status = "Parado garagem" THEN 0
          WHEN status LIKE "Parado %" THEN 1
        ELSE
        0
      END
        AS indicador_veiculo_parado
      FROM
        `rj-smtr-dev.br_rj_riodejaneiro_veiculos.gps_sppo_15_minutos`
      WHERE
        DATA = "{datahora.date()}"
        AND timestamp_gps BETWEEN "{datahora - timedelta(hours=1)}"
        AND "{datahora}" ),
    
    
    -- 2. Puxa dados das rotas (shapes) dos serviços de onibus
      shapes AS (
      SELECT
        t1.*,
        t2.trip_short_name AS servico
      FROM (
        SELECT
          *
        FROM
          `rj-smtr.br_rj_riodejaneiro_gtfs.shapes_geom`
        WHERE
          data_versao = "{data_versao_gtfs}") t1
      INNER JOIN (
        SELECT
          DISTINCT trip_short_name,
          shape_id
        FROM
          `rj-smtr.br_rj_riodejaneiro_gtfs.trips`
        WHERE
          data_versao = "{data_versao_gtfs}") t2
      USING
        (shape_id) ),
    
    
      -- 3. Adiciona indicador de veiculo fora da rota (shape)
      aux_gps_rota AS (
      SELECT
        gps.*,
        shape_id,
        case when ST_DWITHIN(ST_GEOGPOINT(longitude, latitude), shape, 50) then 0 else 1 end AS indicador_veiculo_fora_rota
      FROM
        gps
      LEFT JOIN
        shapes
      USING
        (servico) ),
      gps_rota AS (
      SELECT
        * EXCEPT(rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY id_veiculo, timestamp_gps ORDER BY indicador_veiculo_fora_rota) AS rn
        FROM
          aux_gps_rota )
      WHERE
        rn = 1 ),
    
    
    -- 4. Calcula tempo acumulado de veiculo parado e/ou fora de rota
      gps_acumulado as (select
          g.servico,
          g.id_veiculo,
          g.timestamp_gps,
          g.latitude,
          g.longitude,
          sum(indicador_veiculo_fora_rota) OVER (
            PARTITION BY servico, id_veiculo
            ORDER BY timestamp_gps
            ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
          ) = 10 AS indicador_veiculo_fora_rota_10_min,
          sum(indicador_veiculo_fora_rota) OVER (
            PARTITION BY servico, id_veiculo
            ORDER BY timestamp_gps
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
          ) = 30 AS indicador_veiculo_fora_rota_30_min,
          sum(indicador_veiculo_fora_rota) OVER (
            PARTITION BY servico, id_veiculo
            ORDER BY timestamp_gps
            ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
          ) = 60 AS indicador_veiculo_fora_rota_1_hora,
          sum(indicador_veiculo_parado) OVER (
            PARTITION BY servico, id_veiculo
            ORDER BY timestamp_gps
            ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
          ) = 10 AS indicador_veiculo_parado_10_min,
          sum(indicador_veiculo_parado) OVER (
            PARTITION BY servico, id_veiculo
            ORDER BY timestamp_gps
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
          ) = 30 AS indicador_veiculo_parado_30_min,
          sum(indicador_veiculo_parado) OVER (
            PARTITION BY servico, id_veiculo
            ORDER BY timestamp_gps
            ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
          ) = 60 AS indicador_veiculo_parado_1_hora
        from gps_rota g
        WHERE timestamp_gps between "{(datahora - timedelta(minutes=15))}" and "{datahora}"
    ),
    
    -- 5. Puxa camada de hexagonos que cobrem a cidade
      geometria AS (
      SELECT
        * EXCEPT(geometry),
        ST_GEOGFROMTEXT(geometry) AS tile
      FROM
        `rj-smtr.br_rj_riodejaneiro_geo.h3_res9` ),
      -- 6. Adiciona dados pluviometricos da estacao mais proxima à geometria
      precipitacao_acumulada AS (
      SELECT
        estacao,
        ST_GEOGPOINT(longitude, latitude) AS posicao_estacao,
        t.* EXCEPT(id_estacao)
      FROM (
        SELECT
          *
        FROM
          `datario.clima_pluviometro.taxa_precipitacao_alertario`
        WHERE
          data_particao = "{datahora.date()}"
          AND horario between "{(datahora - timedelta(hours=1)).time()}" and "{datahora.time()}" ) t
      LEFT JOIN (
        SELECT
          *
        FROM
          datario.clima_pluviometro.estacoes_alertario ) e
      USING
        (id_estacao) ),
      cross_geo_precipitacao AS (
      SELECT
        * EXCEPT (rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY tile_id ORDER BY distancia) AS rn
        FROM (
          SELECT
            g.tile_id,
            p.estacao,
            ST_DISTANCE(posicao_estacao, tile) AS distancia
          FROM
            geometria g
          CROSS JOIN
            precipitacao_acumulada p ) )
        where rn = 1
      ),
      geo_precipitacao_acumulada as (
        SELECT
        g.*,
        p.*
      FROM
        cross_geo_precipitacao c
      LEFT JOIN
        geometria g
      USING
        (tile_id)
      LEFT JOIN
        precipitacao_acumulada p
      USING
        (estacao)
      )
    
      -- 7. Junta informações de posicao dos veiculos e precipitacao por tile
    SELECT
      gps.servico,
      gps.id_veiculo,
      gps.timestamp_gps,
      gps.latitude,
      gps.longitude,
      geo.tile,
      geo.tile_id,
      gps.indicador_veiculo_fora_rota_10_min,
      gps.indicador_veiculo_fora_rota_30_min,
      gps.indicador_veiculo_fora_rota_1_hora,
      gps.indicador_veiculo_parado_10_min,
      gps.indicador_veiculo_parado_30_min,
      gps.indicador_veiculo_parado_1_hora,
      geo.acumulado_chuva_15_min,
      geo.acumulado_chuva_1_h,
      geo.acumulado_chuva_4_h,
      geo.acumulado_chuva_24_h,
      geo.estacao as estacao_pluviometro,
      geo.posicao_estacao,
      geo.horario as horario_leitura_estacao
    FROM gps_acumulado gps
    LEFT JOIN
      geo_precipitacao_acumulada geo
    ON
      ST_INTERSECTS(ST_GEOGPOINT(longitude, latitude), tile) = TRUE
    
    """
    print(f"""
Received variables:
    datahora: {datahora}
    data_versao_gtfs: {data_versao_gtfs}

Will run query:
{q}
""")
    
    return bd.read_sql(q, from_file=True)
    

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

    # Carrega dados da operação
    data_versao_gtfs = "2024-01-02" # TODO: atualizar para jan/24
    datahora_atual = datetime.now().replace(second=0, microsecond=0)
    minutos_arredondados = datahora_atual.minute - (datahora_atual.minute % 15)
    datahora_arredondada = datahora_atual.replace(
        minute=minutos_arredondados, second=0, microsecond=0
    )
    if datahora_arredondada > datahora_atual - timedelta(minutes=6):
        datahora = datahora_arredondada - timedelta(minutes=15)
    else:
        datahora = datahora_arredondada

    # print(">>> AQUI 1:", datetime.now())
    df = load_gps(datahora=datahora, data_versao_gtfs=data_versao_gtfs)
    
    # print(">>> AQUI 2:", datetime.now())
    # Calcula os indicadores de cada tile
    df_tile_indicators = (
        df
        .loc[df.groupby(["tile_id", "servico", "id_veiculo"]).timestamp_gps.idxmax()]
        .groupby(["tile_id", "tile", "horario_leitura_estacao"]).agg(
            {
                "acumulado_chuva_15_min": "max",
                "acumulado_chuva_1_h": "max",
                "acumulado_chuva_4_h": "max",
                "id_veiculo": "count",
                "servico": lambda x: ", ".join(list(set(x))),
                "indicador_veiculo_parado_10_min": "sum",
                "indicador_veiculo_fora_rota_10_min": "sum",
                "indicador_veiculo_parado_30_min": "sum",
                "indicador_veiculo_fora_rota_30_min": "sum",
                "indicador_veiculo_parado_1_hora": "sum",
                "indicador_veiculo_fora_rota_1_hora": "sum",
            }
        ).reset_index()
    )
    
    # print(">>> AQUI 3:", datetime.now())
    # Filtra a última medida da estacao
    df_tile_indicators = df_tile_indicators.loc[df_tile_indicators.groupby(["tile_id"]).horario_leitura_estacao.idxmax()]
    df_tile_indicators["horario_leitura_estacao"] = df_tile_indicators.horario_leitura_estacao.astype(str)
    df_tile_indicators.geometry = df_tile_indicators['tile'].dropna().astype(str).apply(loads)
    
    # print(">>> AQUI 4:", datetime.now())
    df_geo = gpd.GeoDataFrame(
        data=df_tile_indicators,
        geometry=df_tile_indicators.geometry,
        crs=4326
    ).drop(columns=["tile"])

    # Instancia o mapa
    st.button("Atualizar dados")
    m = folium.Map(location=[-22.917690, -43.413861], zoom_start=11)
    
    # print(">>> AQUI 5:", datetime.now())
    # Adiciona dados de indice de chuva dos tiles
    colormap = cm.LinearColormap(
        ["green", "yellow", "red"], 
        vmin=0, 
        vmax=100,
        caption="Acumulado de chuva na última hora (mm)"
    )
    colormap.add_to(m)
    colorscale_dict = df_geo.set_index("tile_id")["acumulado_chuva_1_h"]
    
    # print(">>> AQUI 6:", datetime.now())
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
    
    # print(">>> AQUI 7:", datetime.now())
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

    # print(">>> AQUI 8:", datetime.now())
    shapes = load_shapes()

    # print(">>> AQUI 9:", datetime.now())
    # Adiciona rotas ao mapa
    folium.GeoJson(shapes['geometry'], color='gray', weight=1.5, opacity=.8).add_to(m)

    # Ajusta camadas
    folium.TileLayer('cartodbpositron').add_to(m)
    folium.LayerControl().add_to(m)

    # print(">>> AQUI 10:", datetime.now())
    # map_data = st_folium(m, key="mapa", height=600, width=1200)

main()
