FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/.basedosdados/templates && \
    mkdir -p /root/.basedosdados/credentials/ \
    mkdir /tmp/bases


COPY . .

RUN chmod +x script.sh

RUN pip3 install -r requirements.txt

EXPOSE 8502

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["./script.sh"]