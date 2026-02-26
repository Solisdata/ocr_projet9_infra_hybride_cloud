from kafka import KafkaProducer
import json
from datetime import datetime
import random

# Adresse de du broker local
BROKER = '127.0.0.1:19092'
TOPIC = 'client_tickets'

# Creation du producer
producer = KafkaProducer(
    bootstrap_servers=BROKER)


# Creation d'un consumer '
producer = KafkaProducer(
    bootstrap_servers=BROKER)


# générer tickets leatoire
types_demande = ['support technique', 'facturation', 'info générale', 'autre']
priorites = ['basse', 'moyenne', 'haute']

def generer_ticket(ticket_id, client_id):
    return {
        'ticket_id': str(ticket_id),
        'client_id': str(client_id),
        'date_creation': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'demande': random.choice([
            'Mot de passe oublié',
            'Problème de facturation',
            'Question sur les horaires',
            'Changement d’adresse email'
        ]),
        'type_demande': random.choice(types_demande),
        'priorite': random.choice(priorites)
    }

# Exemple : envoyer 50 tickets
for i in range(1, 51):
    ticket = generer_ticket(i, 100+i)
        # Transformer en JSON puis en bytes
    ticket_bytes = json.dumps(ticket).encode('utf-8')
    producer.send(TOPIC, ticket_bytes)
    print(f"Ticket envoyé : {ticket}")

# S’assurer que tout est envoyé
producer.flush() 
producer.close()
print("Tickets envoyés !")