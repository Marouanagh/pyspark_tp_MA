from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col, lit, sum
from pyspark.sql.types import DoubleType  # Add this import
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Exercices PySpark") \
    .getOrCreate()
# --- Exercice 3: Nettoyage de données ---
clients_df = spark.read.csv("/home/toto/clients.csv", header=True, inferSchema=True)

# Fill missing age with the mean age
mean_age = clients_df.select(mean(col("Âge"))).collect()[0][0]
clients_df = clients_df.fillna({"Âge": mean_age})

# Fill missing city with "Inconnue"
clients_df = clients_df.fillna({"Ville": "Inconnue"})

# Drop rows with missing revenue
clients_df = clients_df.na.drop(subset=["Revenu"])

# Explicitly cast columns to ensure proper formatting
clients_df = clients_df.withColumn("Âge", col("Âge").cast(DoubleType()))
clients_df = clients_df.withColumn("Revenu", col("Revenu").cast(DoubleType()))

# Display cleaned DataFrame
clients_df.show()
# --- Exercice 4: Produits les plus chers ---
produits_df = spark.read.csv("/home/toto/produits.csv", header=True, inferSchema=True)

# Get the 3 most expensive products
produits_plus_chers = produits_df.orderBy(col("Prix").desc()).limit(3)

# Display result
produits_plus_chers.show()

# --- Exercice 5: Analyse des transactions ---
transactions_df = spark.read.csv("/home/toto/transactions.csv", header=True, inferSchema=True)

# Calculate total spending per client
total_spending_df = transactions_df.groupBy("Client").agg(sum("Montant").alias("Dépenses_totales"))

# Identify the client with the highest spending
top_spender = total_spending_df.orderBy(col("Dépenses_totales").desc()).first()

# Display total spending per client
total_spending_df.orderBy(col("Dépenses_totales").desc()).show()

# Display the top spender
if top_spender:
    print(f"Client ayant dépensé le plus : {top_spender['Client']} ({top_spender['Dépenses_totales']} unités)")

# --- Exercice 6: Agrégation de données par catégorie ---
stats_by_category = produits_df.groupBy("Catégorie").agg(
    mean(col("Prix")).alias("Prix_moyen"),
    sum(col("Prix")).alias("Prix_total")
)

# Display results
stats_by_category.show()
