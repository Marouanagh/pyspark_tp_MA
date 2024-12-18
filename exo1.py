from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialisation de la session Spark
spark = SparkSession.builder.appName("Analyse des ventes").getOrCreate()

# Chemin vers le fichier CSV
file_path = r"C:\Users\msi\Downloads\ventes.csv"

# Chargement des données CSV
ventes_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Calcul du chiffre d'affaires pour chaque ligne
ventes_df = ventes_df.withColumn("Chiffre_affaires", col("Quantité") * col("Prix_unitaire"))

# Calcul du chiffre d'affaires total
chiffre_affaires_total = ventes_df.agg(sum("Chiffre_affaires").alias("Chiffre_affaires_total")).collect()[0][0]

# Trouver le produit le plus vendu
produit_plus_vendu = ventes_df.groupBy("Produit").agg(sum("Quantité").alias("Quantité_totale")).orderBy(col("Quantité_totale").desc()).first()

print(f"Chiffre d'affaires total : {chiffre_affaires_total} €")
print(f"Produit le plus vendu : {produit_plus_vendu['Produit']} ({produit_plus_vendu['Quantité_totale']} unités)")
