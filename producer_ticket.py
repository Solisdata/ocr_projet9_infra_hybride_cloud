from kafka import KafkaProducer
import json
from datetime import datetime
import random
import time

# Adresse de du broker local
BROKER = '127.0.0.1:19092'
TOPIC = 'client_tickets'

# Creation du producer
producer = KafkaProducer(bootstrap_servers=BROKER)

# générer tickets aléatoire
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
            'Changement adresse email'
        ]),
        'type_demande': random.choice(types_demande),
        'priorite': random.choice(priorites)
    }

# Envoi en flux continu (Ctrl+C pour arrêter)
print("Envoi continu de tickets... (Ctrl+C pour arrêter)")
ticket_id = 1
while True:
    client_id = random.randint(100, 1999)
    ticket = generer_ticket(ticket_id, client_id)
    ticket_bytes = json.dumps(ticket).encode('utf-8')
    producer.send(TOPIC, ticket_bytes)
    print(f"✅ Ticket {ticket_id} envoyé : {ticket}")
    ticket_id += 1
    time.sleep(5)
    #1 ticket toutes les 5 secondes