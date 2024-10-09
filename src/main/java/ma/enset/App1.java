package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App1 {
    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession ss = SparkSession.builder()
                .appName("Spark TP SQL")
                .master("local[*]")
                .getOrCreate();

        // Read the JSON file into a DataFrame with the multiline option
        Dataset<Row> df1 = ss.read()
                .option("multiline", "true")
                .json("products.json");

        // Show the schema of the DataFrame
        df1.printSchema();

        // Display the DataFrame
        df1.show();

        // Stop the Spark session
        ss.stop();
    }
}