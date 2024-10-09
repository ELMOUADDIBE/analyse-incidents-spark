package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AnalyseIncidents {
    public static void main(String[] args) {
        // Initialiser la session Spark
        SparkSession spark = SparkSession.builder()
                .appName("Analyse des Incidents")
                .master("local[*]")
                .getOrCreate();

        // Lire le fichier CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")   // en-tête
                .option("inferSchema", "true") // Inférer automatiquement le schéma des données
                .option("delimiter", ",")   // Définir le délimiteur (par défaut, c'est déjà ",")
                .csv("incidents.csv");

        // Afficher le schéma du DataFrame
        df.printSchema();

        // Afficher les données du DataFrame
        df.show();

        // **1. Afficher le nombre d’incidents par service**

        // Grouper par 'Service' et compter le nombre d'incidents
        Dataset<Row> incidentsParService = df.groupBy("Service").count();

        // Afficher les résultats
        System.out.println("Nombre d'incidents par service :");
        incidentsParService.show();

        // **2. Afficher les deux années où il y a eu le plus d’incidents**

        // Convertir la colonne 'Date' en format date si nécessaire
        Dataset<Row> dfAvecDate = df.withColumn("Date", functions.to_date(df.col("Date"), "yyyy-MM-dd"));

        // Extraire l'année de la colonne 'Date'
        Dataset<Row> dfAvecAnnee = dfAvecDate.withColumn("Annee", functions.year(dfAvecDate.col("Date")));

        // Grouper par 'Annee' et compter le nombre d'incidents
        Dataset<Row> incidentsParAnnee = dfAvecAnnee.groupBy("Annee").count();

        // Trier par le nombre d'incidents en ordre décroissant et prendre les deux premières années
        Dataset<Row> topAnnees = incidentsParAnnee.orderBy(functions.col("count").desc()).limit(2);

        // Afficher les résultats
        System.out.println("Les deux années avec le plus d'incidents :");
        topAnnees.show();

        // Arrêter la session Spark
        spark.stop();
    }
}