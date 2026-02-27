

# Adresse du redpanda broker
BROKER = 'redpanda-0:9092'
TOPIC = 'client_tickets'

# types de demande et priorités
types_demande = ['support technique', 'facturation', 'info générale', 'autre']
priorites = ['basse', 'moyenne', 'haute']

# Délais
TICKET_SLEEP_SECONDS = 5
# temps entre chaque ticket produit



# PySpark / Batch
SPARK_APP_NAME = "KafkaTicketBatchStats_LastHour"
SPARK_MASTER = "local[*]" #job exécuter en local
SPARK_SHUFFLE_PARTITIONS = 2 #nombre de partitions pour calculer en parallèle
WINDOW_HOURS = 1 #toutes les heures


# Mappings
EQUIPE_MAPPING = {
    "support technique": "Equipe Tech",
    "facturation": "Equipe Facturation",
    "info générale": "Equipe Generale",
    "autre": "Equipe Generale"
}