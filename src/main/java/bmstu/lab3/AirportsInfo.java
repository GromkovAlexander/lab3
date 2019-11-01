package bmstu.lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsInfo {

    private final static String COMMA = ",";
    private final static String QUOTES = "\"";
    private final static String EMPTY = "";
    private static final int COLUMN_AIRPORT_CODE = 0;
    private static final int COLUMN_AIRPORT_DESCRIPTION = 1;

    public static JavaRDD<String> loadData(JavaSparkContext sc, String path) {
        JavaRDD<String> airports = sc.textFile(path);
        String title = airports.first();
        return airports.filter(s -> !s.equals(title));
    }

    public static String deleteQuotes(String s) {
        return s.replaceAll(QUOTES, EMPTY);
    }

    public static String getValue(String s, int pos) {
        String[] columns = s.split(COMMA, 2);
        return deleteQuotes(columns[pos]);
    }

    public static JavaPairRDD<Integer, String> sortKV(JavaRDD<String> file) {
        JavaPairRDD<Integer, String> kv = file.mapToPair(
                s -> new Tuple2<>(
                        Integer.parseInt(getValue(s, COLUMN_AIRPORT_CODE)),
                        getValue(s, COLUMN_AIRPORT_DESCRIPTION)
                )
        );

        return kv;
    }

}
