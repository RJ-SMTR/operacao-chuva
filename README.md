# Monitoramento do impacto de chuvas na operação de transportes

Ferramenta de apoio ao acompanhamento da operação de serviços de
transporte municipal em dias de chuva.

- 🌧️ Precipitação nos últimos 15 min e 1 hora
- 🚍 Veículos parados e fora de rota nos últimos 10 min e 30 min
- 🌊 [Alagamentos em tempo
  real](<https://api.dados.rio/v2/clima_alagamento/>) (*A desenvolver*)
- 🛜 Posição dos veículos em tempo real (*A desenvolver*)

![Interface da ferramenta](preview.jpeg)

## Desenvolvimento

1. Instale as seguintes ferramentas:

- [gcloud](https://cloud.google.com/sdk/docs/install?hl=pt-br#mac)
- [kubectl](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl?hl=pt-br)*

\* Utilize a seguinte configuração:

```sh
gcloud container clusters get-credentials rj-smtr --region=us-central1-c
```

2. Exponha o redis (banco contendo o cache dos dados) no seu local:

```sh
kubectl port-forward svc/redis -n operacao-chuva-staging 6379:6379
```

3. Num novo terminal, ative e configure o ambiente:

```sh
# Crie um ambiente local
python -m venv .venv
# Instancie o ambiente
. .venv/bin/activate
# Instale as dependências
pip install -r requirements.txt
# Instancie as variaveis de ambiente
source .env-local
```

5. Execute o aplicativo localmente:

```sh
streamlit run src/app.py
```

Pronto! O aplicativo estará disponível no seu navegador:
[http://localhost:8501/](http://localhost:8501/)
