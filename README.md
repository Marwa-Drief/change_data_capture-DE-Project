

#  CDC Streaming avec Kafka, Debezium, Spark et Grafana

## Description du projet

Ce projet met en place une **chaîne de traitement temps réel** basée sur **Change Data Capture (CDC)**.
Chaque transaction insérée dans **PostgreSQL** est automatiquement capturée par **Debezium**, envoyée à **Kafka**, traitée par **Spark Structured Streaming**, et exposée sous forme de métriques Prometheus visualisables dans **Grafana**.

###  Pipeline complet :

1. **PostgreSQL** : Base de données source contenant les transactions financières.
2. **Debezium** : Capture les changements (INSERT, UPDATE, DELETE) et les publie dans Kafka.
3. **Kafka** : Sert de bus de messages distribué.
4. **Spark Structured Streaming** : Consomme les événements Kafka, transforme les données, expose des métriques vers Prometheus.
5. **Prometheus** : Scrape et stocke les métriques exportées par Spark.
6. **Grafana** : Visualisation et tableaux de bord temps réel.

---

##  Prérequis

* Docker & Docker Compose
* Python 3.8+
* Accès internet (pour télécharger les images Docker)
* `pip install faker psycopg2`

---

##  Lancer l’environnement complet

### 1. Démarrer l’infrastructure

```bash
docker-compose up -d
```

Cela va lancer :

* Zookeeper (2181)
* Kafka Broker (9092 / 29092)
* Control Center Confluent (9021)
* PostgreSQL (5432)
* Debezium Connect (8093)
* Debezium UI (8080)
* Spark (8081 / 4040)
* Prometheus (9090)
* Grafana (3000)

---

### 2. Créer la table et insérer des données dans PostgreSQL

Le script **`transactions_generator.py`** crée automatiquement la table `transactions` et insère une transaction factice.

Exécute-le avec :

```bash
python transactions_generator.py
```

 Exemple de transaction générée :

```json
{
  "transactionId": "9b8a4d72-4f6c-41d4-bf9b-8f1d45c37d55",
  "userId": "johndoe",
  "timestamp": 1694091234,
  "amount": 450.75,
  "currency": "USD",
  "city": "Paris",
  "country": "France",
  "merchantName": "ACME Corp",
  "paymentMethod": "credit_card",
  "ipAddress": "192.168.0.1",
  "voucherCode": "DISCOUNT10",
  "affiliateId": "aa9b-1234-xyz"
}
```

---

### 3. Configurer Debezium pour PostgreSQL

Déclare un connecteur via l’API REST de Debezium :

```bash
curl -X POST http://localhost:8093/connectors/ -H "Content-Type: application/json" -d '{
  "name": "postgres-transactions-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "financial_db",
    "database.server.name": "cdc",
    "table.include.list": "public.transactions",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot"
  }
}'
```

 Kafka va maintenant recevoir les changements sur le topic :
`cdc.public.transactions`

---

### 4. Lancer le job Spark

Lancer ton script Spark CDC :

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  spark_cdc_consumer.py
```

📍 Ce job :

* Consomme `cdc.public.transactions` depuis Kafka
* Décode les données Debezium
* Expose des métriques sur `http://localhost:8081/metrics`

---

### 5. Vérifier Prometheus

Ouvre [http://localhost:9090](http://localhost:9090) et vérifie que les métriques Spark sont bien scrappées.
Exemples de métriques disponibles :

* `cdc_transactions_total` → compteur global des transactions
* `cdc_current_amount_eur` → dernier montant par ville/marchand
* `cdc_tps` → transactions par seconde
* `cdc_transaction_amount_euro` → histogramme des montants

---

### 6. Visualiser dans Grafana

1. Accède à Grafana : [http://localhost:3000](http://localhost:3000)

   * **User** : admin
   * **Password** : admin
2. Ajoute Prometheus comme source de données (`http://prometheus:9090`)
3. Crée un dashboard avec panels :

   * Transactions par seconde (TPS)
   * Volume de transactions par pays
   * Montant moyen par ville/marchand
   * Histogramme des montants

---

##  Tests

* Insérer manuellement une transaction dans PostgreSQL :

```bash
docker exec -it postgres psql -U postgres -d financial_db  
```

```sql
INSERT INTO transactions(transaction_id, user_id, timestamp, amount, currency, city, country, merchant_name, payment_method, ip_address, affiliateId, voucher_code)
VALUES ('tx123', 'alice', NOW(), 99.99, 'USD', 'London', 'UK', 'Shopify', 'credit_card', '192.168.1.10', 'aff-001', 'DISCOUNT10');
```

* Vérifie ensuite dans Grafana que la métrique **`cdc_transactions_total`** a bien été incrémentée.

---

##  Services exposés

| Service       | Port local  |
| ------------- | ----------- |
| PostgreSQL    | 5432        |
| Kafka Broker  | 9092        |
| Kafka Control | 9021        |
| Debezium      | 8093        |
| Debezium UI   | 8080        |
| Spark Master  | 8081 / 4040 |
| Prometheus    | 9090        |
| Grafana       | 3000        |

---

## Améliorations futures

* Ajouter **alerting Prometheus** (ex: alerte si TPS > seuil)
* Déployer sur Kubernetes avec Helm
* Gérer le **multi-database CDC** (ex: plusieurs tables)

