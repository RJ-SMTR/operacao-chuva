from datetime import datetime
from google.cloud import bigquery
from shapely.wkt import loads
from celery import Celery
from datetime import datetime, timedelta
from redis_sr import RedisSR


import os
import geopandas as gpd
import pandas as pd
import traceback
import folium
import branca.colormap as cm
import folium
import folium.plugins as plugins
import pytz


app = Celery("main", broker=os.getenv("REDIS_CELERY"))
app.conf.timezone = "UTC"


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(180.0, main.s(), name="Update data every 3 minutes")
    sender.add_periodic_task(180.0, cache_mapa.s(), name="Update map every 3 minutes")


# V1: loading shapes in every update was very slow
# New shapes should be parsed beforehand to geojson format with
# the function below
# def load_shapes():
# # Carrega dados de rotas (shapes)
# shapes = pd.read_csv("src/data/shapes.txt", dtype={
#                 'shape_id': 'str',
#                 'shape_pt_lat': 'float',
#                 'shape_pt_lon': 'float',
#                 'shape_pt_sequence': 'Int64',
#                 'shape_dist_traveled': 'float',
#             })
# shapes = gpd.GeoDataFrame(shapes,
#         geometry=gpd.points_from_xy(shapes.shape_pt_lon, shapes.shape_pt_lat)
#     ).set_crs(epsg=4326)
# shapes.sort_values(['shape_id','shape_pt_sequence'], inplace=True)
# shapes = (
#     shapes[["shape_id", "shape_pt_lat", "shape_pt_lon"]]
#     .groupby("shape_id")
#     .agg(list)
#     .apply(lambda x: LineString(zip(x[1], x[0])), axis=1)
# )

# shapes = gpd.GeoDataFrame(
#     data=shapes.index,
#     geometry = shapes.values,
#     crs=4326
# )
# shapes['shape_id'] = shapes.shape_id.astype(str)
# shapes.to_json()
# return shapes


def load_shapes():
    return gpd.read_file("data/shapes.geojson")


def create_map(data=None):
    df_geo = data
    # df_geo = gpd.read_file('dataframe.geojson')
    # Instancia o mapa
    shapes = load_shapes()
    m = folium.Map(location=[-22.917690, -43.413861], zoom_start=11)

    # Adiciona dados de indice de chuva dos tiles
    colormap = cm.LinearColormap(
        ["green", "yellow", "red"],
        vmin=0,
        vmax=100,
        caption="Acumulado de chuva na última hora (mm)",
    )
    colormap.add_to(m)
    colorscale_dict = df_geo.set_index("tile_id")["acumulado_chuva_1_h"].sort_values()

    popup = folium.GeoJsonPopup(
        fields=[
            "horario_leitura_estacao",
            "acumulado_chuva_15_min",
            "acumulado_chuva_1_h",
            "servicos",
            "indicador_veiculo_parado_10_min",
            "indicador_veiculo_parado_30_min",
            "indicador_veiculo_parado_1_hora",
            "indicador_veiculo_fora_rota_10_min",
            "indicador_veiculo_fora_rota_30_min",
            "indicador_veiculo_fora_rota_1_hora",
            "tile_id",
        ],
        aliases=[
            "🌧️ Hora da leitura:",
            "🌧️ Acumulado 15min:",
            "🌧️ Acumulado 1h:",
            "🚍 Serviços em operação: ",
            "🛑 Veículos parados 10 min:",
            "🛑 Veículos parados 30 min:",
            "🛑 Veículos parados 1 hora:",
            "⤴️ Veículos desviados 10 min:",
            "⤴️ Veículos desviados 30 min:",
            "⤴️ Veículos desviados 1 hora:",
            "📍 ID do polígono: ",
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
            "weight": 0.2,
            "fillOpacity": 0.7,
        },
        color="acumulado_chuva_15_min",
        weight=2.5,
        opacity=1,
        popup=popup,
    ).add_to(m)

    df_geo["total_veiculo_problema"] = (
        df_geo.indicador_veiculo_parado_10_min
        + df_geo.indicador_veiculo_parado_30_min
        + df_geo.indicador_veiculo_parado_1_hora
        + df_geo.indicador_veiculo_fora_rota_10_min
        + df_geo.indicador_veiculo_fora_rota_30_min
        + df_geo.indicador_veiculo_fora_rota_1_hora
    )

    # Adiciona icones de qtd de veiculos parados/fora da rota
    for i in range(0, len(df_geo)):

        pin_count = df_geo.iloc[i].total_veiculo_problema

        pin = False

        if pin_count > 10:
            pin = True
            pin_color = "#085d73"

        elif pin_count > 5:
            pin = True
            pin_color = "#1f97b5"

        elif pin_count > 0:
            pin = True
            pin_color = "#5cdafa"

        if pin == True:
            folium.Marker(
                location=[
                    df_geo.iloc[i].geometry.centroid.y,
                    df_geo.iloc[i].geometry.centroid.x,
                ],
                icon=plugins.BeautifyIcon(
                    icon="arrow-down",
                    icon_shape="marker",
                    number=str(pin_count),
                    border_color=pin_color,
                    background_color=pin_color,
                ),
            ).add_to(m)

    # Adiciona rotas ao mapa
    folium.GeoJson(shapes["geometry"], color="gray", weight=1.5, opacity=0.8).add_to(m)

    # Ajusta camadas
    folium.TileLayer("cartodbpositron").add_to(m)
    folium.LayerControl().add_to(m)
    return m


@app.task
def cache_mapa():
    datahora = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).replace(
        second=0, microsecond=0, tzinfo=None
    )
    redis = RedisSR.from_url(os.getenv("CACHE_OPERACAO_CHUVA"))
    try:
        data = redis.get("data")
        mapa = create_map(data=data)
        redis.set("last_map", mapa)
        redis.set("last_map_timestamp", str(datahora))
    except:
        pass


def load_gps(datahora, data_versao_gtfs):
    # Carrega dados da operação
    gps = f"""
    -- 1. Puxa dados de posição de GPS dos ônibus e flag indicativa de parada
    with gps AS (
      SELECT
        servico,
        id_veiculo,
        latitude,
        longitude,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo,
        timestamp_gps,
        CASE
          WHEN status = "Parado garagem" THEN 0
          WHEN status LIKE "Parado %" THEN 1
        ELSE
        0
      END
        AS indicador_veiculo_parado
      FROM
        `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo_15_minutos`
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
          `rj-smtr.gtfs.shapes_geom`
        WHERE
          feed_start_date = "{data_versao_gtfs}") t1
      INNER JOIN (
        SELECT
          DISTINCT trip_short_name,
          shape_id
        FROM
          `rj-smtr.gtfs.trips`
        WHERE
          feed_start_date = "{data_versao_gtfs}") t2
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
          g.posicao_veiculo,
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
    )
    SELECT 
      *
    FROM gps_acumulado
    """
    client = bigquery.Client(project="rj-smtr")
    return client.query(gps).to_dataframe()


def load_tiles(datahora):

    geo_tiles = f"""
    -- 5. Puxa camada de hexagonos que cobrem a cidade
    with
    ultima_medicao_alertario AS (
      SELECT
        MAX (data_medicao) as ultima_data
      FROM
        `datario.clima_pluviometro.taxa_precipitacao_alertario_5min` ),
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
          `datario.clima_pluviometro.taxa_precipitacao_alertario_5min`
        WHERE
          data_particao = "{datahora.date()}"
          AND data_medicao BETWEEN DATETIME_SUB((select ultima_data from ultima_medicao_alertario), INTERVAL 15 MINUTE) AND (select ultima_data from ultima_medicao_alertario) ) t
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
    
    SELECT 
      * EXCEPT(estacao, data_medicao),
      estacao as estacao_pluviometro,
      data_medicao as horario_leitura_estacao
    FROM
      geo_precipitacao_acumulada
    """
    client = bigquery.Client(project="rj-smtr")
    return client.query(geo_tiles).to_dataframe()


def get_gps_data_last_update(datahora):
    query = f"""
    SELECT 
      MAX(timestamp_gps)
    FROM 
      `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo_15_minutos`
    WHERE
      data >= "{(datahora - timedelta(hours=24)).date()}"
  """
    client = bigquery.Client(project="rj-smtr")
    last_update = client.query(query=query).to_dataframe().iloc[0, 0]
    return last_update



def get_rain_data_last_update(datahora):
    query = f"""
      SELECT
        MAX (data_medicao) as ultima_data
      FROM
        `datario.clima_pluviometro.taxa_precipitacao_alertario_5min`
    """
    client = bigquery.Client(project="rj-smtr")
    last_update = client.query(query=query).to_dataframe().iloc[0, 0]
    return last_update


@app.task
def main():
    try:
        redis = RedisSR.from_url(os.getenv("CACHE_OPERACAO_CHUVA"))
        # Carrega dados da operação
        data_versao_gtfs = "2024-01-02"  # TODO: atualizar para jan/24
        datahora_atual = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).replace(
            second=0, microsecond=0, tzinfo=None
        )
        minutos_arredondados = datahora_atual.minute - (datahora_atual.minute % 15)
        datahora_arredondada = datahora_atual.replace(
            minute=minutos_arredondados, second=0, microsecond=0
        )

        if datahora_arredondada > datahora_atual - timedelta(minutes=6):
            datahora = datahora_arredondada - timedelta(minutes=15)
        else:
            datahora = datahora_arredondada

        # datahora -= timedelta(hours=3)

        print(">>> Loading gps:", datetime.now())
        df_gps = load_gps(datahora=datahora, data_versao_gtfs=data_versao_gtfs)
        gps_data_last_update = get_gps_data_last_update(datahora)
        df_gps.posicao_veiculo = df_gps.posicao_veiculo.astype(str).apply(loads)
        df_gps_geo = gpd.GeoDataFrame(
            data=df_gps, geometry=df_gps.posicao_veiculo, crs=4326
        )
        if len(df_gps) != 0:
            redis.set("last_df_gps", df_gps)
            redis.set(
                "last_df_gps_timestamp", gps_data_last_update.strftime("%d/%m/%Y %H:%M")
            )
        print(f"Built gps geo!\nColumns:{df_gps_geo.columns}\nSize:{len(df_gps_geo)}")
        print("Loading tiles")
        df_tiles = load_tiles(datahora=datahora)
        df_tiles.tile = df_tiles.tile.astype(str).apply(loads)
        df_tiles.horario_leitura_estacao = pd.to_datetime(df_tiles.horario_leitura_estacao)
        df_tiles_geo = gpd.GeoDataFrame(data=df_tiles, geometry=df_tiles.tile, crs=4326)
        print(
            f"Built tiles geo!\nColumns:{df_tiles_geo.columns}\nSize:{len(df_tiles_geo)}"
        )
        df = df_gps_geo.sjoin(df_tiles_geo, how="left", predicate="intersects")
        df.tile = df.tile.astype(str)
        df.posicao_veiculo = df.posicao_veiculo.astype(str)
        print(f"Joined gps and tiles, got data:\n{df.head(10)}\n df size is {len(df)}")
        print(f"df columns are:\n{df.columns}")

        # Calcula os indicadores de cada tile
        df_tile_indicators = df.loc[
            df.groupby(["tile_id", "servico", "id_veiculo"]).timestamp_gps.idxmax()
        ].reset_index()
        df_tile_indicators = df_tile_indicators.loc[
            df_tile_indicators.groupby(
                ["tile_id", "servico", "id_veiculo"]
            ).horario_leitura_estacao.idxmax()
        ].reset_index()
        df_tile_indicators = (
            df_tile_indicators.groupby(["tile_id", "tile", "servico"])
            .agg(
                {
                    "horario_leitura_estacao": "max",
                    "acumulado_chuva_15min": "max",
                    "acumulado_chuva_1h": "max",
                    "acumulado_chuva_4h": "max",
                    "indicador_veiculo_parado_10_min": "sum",
                    "indicador_veiculo_fora_rota_10_min": "sum",
                    "indicador_veiculo_parado_30_min": "sum",
                    "indicador_veiculo_fora_rota_30_min": "sum",
                    "indicador_veiculo_parado_1_hora": "sum",
                    "indicador_veiculo_fora_rota_1_hora": "sum",
                }
            )
            .reset_index()
        )

        df_tile_indicators["total_veiculo_problema"] = (
            df_tile_indicators.indicador_veiculo_parado_10_min
            + df_tile_indicators.indicador_veiculo_parado_30_min
            + df_tile_indicators.indicador_veiculo_parado_1_hora
            + df_tile_indicators.indicador_veiculo_fora_rota_10_min
            + df_tile_indicators.indicador_veiculo_fora_rota_30_min
            + df_tile_indicators.indicador_veiculo_fora_rota_1_hora
        )

        servicos_dict = {}

        for i, row in df_tile_indicators.iterrows():
            if row["tile_id"] not in servicos_dict:
                servicos_dict[row["tile_id"]] = []
            servicos_dict[row["tile_id"]].append(
                (row["servico"], row["total_veiculo_problema"])
            )

        for tile_id in servicos_dict:
            lista = servicos_dict[tile_id]
            lista.sort(key=lambda x: x[1], reverse=True)
            s = ""
            for servico, total in lista:
                s += f"{servico}: {total}, "
            servicos_dict[tile_id] = s


        servicos_df = pd.DataFrame(
            servicos_dict.items(), columns=["tile_id", "servicos"]
        )
        df_tile_indicators = df_tile_indicators.merge(
            servicos_df, how="inner", on="tile_id"
        )
        # display(servicos_df)
        df_tile_indicators = (
            df_tile_indicators.groupby(["tile_id", "tile"])
            .agg(
                {
                    "servicos": "max",
                    "horario_leitura_estacao": "max",
                    "acumulado_chuva_15min": "max",
                    "acumulado_chuva_1h": "max",
                    "acumulado_chuva_4h": "max",
                    "indicador_veiculo_parado_10_min": "sum",
                    "indicador_veiculo_fora_rota_10_min": "sum",
                    "indicador_veiculo_parado_30_min": "sum",
                    "indicador_veiculo_fora_rota_30_min": "sum",
                    "indicador_veiculo_parado_1_hora": "sum",
                    "indicador_veiculo_fora_rota_1_hora": "sum",
                }
            )
            .reset_index()
        )

        # df_tile_indicators = (
        #     df
        #     .loc[df.groupby(["tile_id", "servico", "id_veiculo"]).timestamp_gps.idxmax()]
        #     .groupby(["tile_id", "tile", "horario_leitura_estacao"]).agg(
        #         {
        #             "acumulado_chuva_15_min": "max",
        #             "acumulado_chuva_1_h": "max",
        #             "acumulado_chuva_4_h": "max",
        #             "id_veiculo": "count",
        #             "servico": lambda x: ", ".join(list(set(x))),
        #             "indicador_veiculo_parado_10_min": "sum",
        #             "indicador_veiculo_fora_rota_10_min": "sum",
        #             "indicador_veiculo_parado_30_min": "sum",
        #             "indicador_veiculo_fora_rota_30_min": "sum",
        #             "indicador_veiculo_parado_1_hora": "sum",
        #             "indicador_veiculo_fora_rota_1_hora": "sum",
        #         }
        #     ).reset_index()
        # )

        # Filtra a última medida da estacao
        # df_tile_indicators = df_tile_indicators.loc[df_tile_indicators.groupby(["tile_id"]).horario_leitura_estacao.idxmax()]
        # df_tile_indicators["horario_leitura_estacao"] = df_tile_indicators.horario_leitura_estacao.astype(str)
        # df_tile_indicators[
        #     "horario_leitura_estacao"
        # ] = df_tile_indicators.horario_leitura_estacao.dt.total_seconds().apply(
        #     lambda s: f"{s // 3600:02.0f}:{(s % 3600) // 60:02.0f}"
        # )
        df_tile_indicators["geometry"] = (
            df_tile_indicators["tile"].dropna().astype(str).apply(loads)
        )

        df_geo = gpd.GeoDataFrame(
            data=df_tile_indicators, geometry=df_tile_indicators.geometry, crs=4326
        ).drop(columns=["tile"])

        if len(df_geo) == 0:
            redis.set("last_empty_data", str(datahora))

        else:
            redis.set("data", df_geo)
            redis.set("last_update", gps_data_last_update.strftime("%d/%m/%Y %H:%M"))
            redis.set(
                "last_rain_update",
                get_rain_data_last_update(datahora).strftime("%d/%m/%Y %H:%M"),
            )

    except Exception as e:
        now = str(datahora)
        stack_trace = traceback.format_exc()
        last_crash = {now: stack_trace}

        redis.set("last_crash", last_crash)


if __name__ == "__main__":
    main()
    cache_mapa()
