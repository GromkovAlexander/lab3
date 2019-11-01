package bmstu.lab3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsInfo {

    private final static String COMMA = ",";

    public static JavaRDD<String> loadData(JavaSparkContext sc, String path) {
        JavaRDD<String> airports = sc.textFile(path);
        String title = airports.first();
        return airports.filter(line -> !line.equals(title));
    }

    public static String getValue(String s, int pos) {
        String[] columns = s.split(COMMA, 2);
        return columns[pos];
    }

    public static JavaRDD<String> sortKV(JavaRDD<String> file) {
        JavaRDD<String> kv = file.mapToPair(
                s -> new Tuple2<>(
                        Integer.parseInt(getValue(s, 0)),
                        getValue(s, 1)
                )
        );

    }

}
