FROM jupyter/pyspark-notebook

USER root

RUN apt-get update && \
    apt-get install -y wget git netcat && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

WORKDIR /home/jovyan/scripts