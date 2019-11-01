package bmstu.lab3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInfo {

    public static JavaRDD<String> loadData(JavaSparkContext sc, String path) {
        JavaRDD<String> airports = sc.textFile(path);
        String title = airports.first();
        return airports.filter(line -> !line.equals(title));
    }

    public static JavaRDD<String> sortKV(JavaRDD<String> file) {
        JavaRDD<String> kv = file.mapToPair(
                
        );

    }

}
