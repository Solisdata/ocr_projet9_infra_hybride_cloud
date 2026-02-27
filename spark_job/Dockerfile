#image de base du containeur 
FROM python:3.11-slim

USER root

RUN apt-get update && apt-get install -y default-jdk procps && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

# Copier requirements et installer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code du producer
COPY spark_job/spark_job.py .

# Copier le fichier de config
COPY config.py .

# Commande par défaut : lorsque que démarre le conteneur, le script se lance
CMD ["python", "spark_job.py"]