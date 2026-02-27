#image de base du containeur 
FROM python:3.11-slim

WORKDIR /app

# Copier requirements et installer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code du producer
COPY producer/producer_ticket.py .

# Copier le fichier de config
COPY config.py .

# Commande par défaut : lorsque que démarre le conteneur, le script se lance
CMD ["python", "producer_ticket.py"]