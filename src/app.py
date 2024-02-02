
import streamlit as st
import os
from streamlit_folium import folium_static
from redis_sr import RedisSR
from shapely.wkt import loads
import geopandas as gpd

def set_page_config():
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

    st.button("Atualizar dados")

def render_map_data(mapa=None, df_gps=None):
    map_data = folium_static(mapa, height=600, width=1200)

    df_gps['total_veiculo_problema'] = (
      df_gps.indicador_veiculo_parado_10_min + 
      df_gps.indicador_veiculo_parado_30_min +
      df_gps.indicador_veiculo_parado_1_hora +
      df_gps.indicador_veiculo_fora_rota_10_min + 
      df_gps.indicador_veiculo_fora_rota_30_min +
      df_gps.indicador_veiculo_fora_rota_1_hora)
    
    st.dataframe(df_gps.groupby(by=['servico'])[['total_veiculo_problema', 'indicador_veiculo_parado_10_min', 'indicador_veiculo_parado_30_min',
                    'indicador_veiculo_parado_1_hora', 'indicador_veiculo_fora_rota_10_min', 'indicador_veiculo_fora_rota_30_min',
                      'indicador_veiculo_fora_rota_1_hora']].sum().sort_values(by=['total_veiculo_problema'], ascending=False))

def main():
    set_page_config()
    redis = RedisSR.from_url(os.getenv('CACHE_OPERACAO_CHUVA'))
    last_update = redis.get('last_update')
    last_update_rain = redis.get('last_rain_update')
    try:        
        mapa = redis.get('last_map')
        df_gps = redis.get('last_df_gps')
        render_map_data(mapa=mapa, df_gps=df_gps)
        st.markdown(f"Última atualização GPS: {last_update}")          
        st.markdown(f"Última atualização meteorológica: {last_update_rain}")  
        redis.set('last_successful_map', mapa)
        redis.set(
            'last_successful_render', 
            last_update)
        redis.set(
            'last_successful_df_gps', 
            df_gps)
    except:
        mapa = redis.get('last_successful_map')
        df_gps = redis.get('last_successful_df_gps')
        render_map_data(mapa=mapa, df_gps=df_gps)
        st.markdown(f"Última atualização: {last_update}")

if __name__ == '__main__':
    main()
