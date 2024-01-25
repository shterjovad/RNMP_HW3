FROM spark:3.5.0-scala2.12-java11-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    apt-get install -y r-base r-base-dev; \
    rm -rf /var/lib/apt/lists/*; \
    pip3 install --upgrade pip;
RUN pip3 install --no-cache-dir jupyterlab pyspark numpy pandas scikit-learn ipykernel; \
    python3 -m ipykernel install --user; \
    mkdir -p /home/spark/.local && chown -R spark:spark /home/spark


ENV R_HOME /usr/lib/R
ENV PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

USER spark

WORKDIR /app