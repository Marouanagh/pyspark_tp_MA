from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Analyse des utilisateurs") \
    .getOrCreate()

# Load the JSON file with proper options
utilisateurs_df = spark.read.option("multiline", "true").json("/home/toto/utilisateurs.json")

# Check the schema to confirm data is loaded correctly
utilisateurs_df.printSchema()

# Calculate the average age
age_moyen = utilisateurs_df.select(mean(col("âge"))).collect()[0][0]

# Count users by city
utilisateurs_par_ville = utilisateurs_df.groupBy("ville").count()

# Find the youngest user
plus_jeune_utilisateur = utilisateurs_df.orderBy(col("âge").asc()).first()

# Display results
print(f"Âge moyen : {age_moyen:.1f} ans")
utilisateurs_par_ville.show()
print(f"Plus jeune utilisateur : {plus_jeune_utilisateur['nom']} ({plus_jeune_utilisateur['âge']} ans)")

# Stop the Spark session
spark.stop()
