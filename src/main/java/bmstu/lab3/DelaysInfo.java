package bmstu.lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class DelaysInfo {

    private final static String COMMA = ",";
    private final static String QUOTES = "\"";
    private final static String EMPTY = "";

    private static final int COUNT_AIRPORT_COLUMNS = 23;
    private static final int AIRPORT_ID_FROM = 23;
    private static final int AIRPORT_ID_TO = 23;


    public static String deleteQuotes(String s) {
        return s.replaceAll(QUOTES, EMPTY);
    }

    public static String getValue(String s, int pos) {
        String[] columns = s.split(COMMA, COUNT_AIRPORT_COLUMNS);
        return deleteQuotes(columns[pos]);
    }


    public static JavaPairRDD<Integer, Integer> flightsFromTo(JavaRDD<String> file) {
        JavaPairRDD<Integer, Integer> kv = file.mapToPair(
                s -> new Tuple2<>(
                        Integer.parseInt(getValue(s, COLUMN_AIRPORT_CODE)),
                        Integer.parseInt(getValue(s, COLUMN_AIRPORT_DESCRIPTION))
                )
        );

        return kv;
    }

}
