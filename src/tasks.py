import os
import geopandas as gpd
import traceback

from google.cloud import bigquery
from shapely.wkt import loads
from celery import Celery
from datetime import datetime, timedelta
from redis_sr import RedisSR

app = Celery('main', broker=os.getenv('REDIS_CELERY'))
app.conf.timezone = 'UTC'

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(60.0, main.s(), name='Update every minute')

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
    client = bigquery.Client(project='rj-smtr-dev')   
    return client.query(gps).to_dataframe()

def load_tiles(datahora):
    
    geo_tiles = f"""
    -- 5. Puxa camada de hexagonos que cobrem a cidade
    with geometria AS (
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
    
    SELECT 
      * EXCEPT(estacao, horario),
      estacao as estacao_pluviometro,
      horario as horario_leitura_estacao
    FROM
      geo_precipitacao_acumulada
    """
    client = bigquery.Client(project='rj-smtr-dev')   
    return client.query(geo_tiles).to_dataframe()

def get_gps_data_last_update():
    query = """
    SELECT 
      MAX(timestamp_gps)
    FROM 
      `rj-smtr-dev.br_rj_riodejaneiro_veiculos.gps_sppo_15_minutos`
  """
    client = bigquery.Client(project='rj-smtr-dev')
    return client.query(query=query).to_dataframe().iloc[0,0]

@app.task
def main():
    try:
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
      
      datahora -= timedelta(hours=3)

      print(">>> Loading gps:", datetime.now())
      df_gps = load_gps(datahora=datahora, data_versao_gtfs=data_versao_gtfs)
      gps_data_last_update = get_gps_data_last_update()
      df_gps.posicao_veiculo = df_gps.posicao_veiculo.astype(str).apply(loads)
      df_gps_geo = gpd.GeoDataFrame(
          data=df_gps,
          geometry=df_gps.posicao_veiculo,
          crs=4326
          )
      print(f'Built gps geo!\nColumns:{df_gps_geo.columns}\nSize:{len(df_gps_geo)}')
      print('Loading tiles')
      df_tiles=load_tiles(datahora=datahora)
      df_tiles.tile = df_tiles.tile.astype(str).apply(loads)
      df_tiles.horario_leitura_estacao = df_tiles.horario_leitura_estacao.astype("timedelta64[ns]")
      df_tiles_geo = gpd.GeoDataFrame(
          data=df_tiles,
          geometry=df_tiles.tile,
          crs=4326
          )
      print(f'Built tiles geo!\nColumns:{df_tiles_geo.columns}\nSize:{len(df_tiles_geo)}')
      df = df_gps_geo.sjoin(df_tiles_geo, how='left', predicate='intersects')
      df.tile = df.tile.astype(str)
      df.posicao_veiculo = df.posicao_veiculo.astype(str)
      print(f'Joined gps and tiles, got data:\n{df.head(10)}\n df size is {len(df)}')
      print(f"df columns are:\n{df.columns}")

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
      
      # Filtra a última medida da estacao
      df_tile_indicators = df_tile_indicators.loc[df_tile_indicators.groupby(["tile_id"]).horario_leitura_estacao.idxmax()]
      # df_tile_indicators["horario_leitura_estacao"] = df_tile_indicators.horario_leitura_estacao.astype(str)
      df_tile_indicators["horario_leitura_estacao"] = df_tile_indicators.horario_leitura_estacao.dt.total_seconds().apply(lambda s: f'{s // 3600:02.0f}:{(s % 3600) // 60:02.0f}')
      df_tile_indicators['geometry'] = df_tile_indicators['tile'].dropna().astype(str).apply(loads)
      
      df_geo = gpd.GeoDataFrame(
          data=df_tile_indicators,
          geometry=df_tile_indicators.geometry,
          crs=4326
      ).drop(columns=["tile"])
      
      redis = RedisSR.from_url(os.getenv('CACHE_OPERACAO_CHUVA'))

      if len(df_geo) == 0:
          redis.set('last_empty_data', str(datetime.now() - timedelta(hours=3)))

      else:      
        redis.set('data', df_geo)
        redis.set('last_update', gps_data_last_update.strftime("%d/%m/%Y %H:%M"))
    

    except Exception:
        
        now = str(datetime.now() - timedelta(hours=3))
        stack_trace = traceback.format_exc()
        last_crash = {now: stack_trace}

        redis.set('last_crash', last_crash)