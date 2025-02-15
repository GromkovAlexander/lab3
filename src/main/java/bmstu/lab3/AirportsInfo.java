package bmstu.lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class AirportsInfo {

    private final static String COMMA = ",";
    private final static String QUOTES = "\"";
    private final static String EMPTY = "";

    private static final int COUNT_AIRPORT_COLUMNS = 2;
    private static final int COLUMN_AIRPORT_CODE = 0;
    private static final int COLUMN_AIRPORT_DESCRIPTION = 1;

    public static String deleteQuotes(String s) {
        return s.replaceAll(QUOTES, EMPTY);
    }

    public static String getValue(String s, int pos) {
        String[] columns = s.split(COMMA, COUNT_AIRPORT_COLUMNS);
        return deleteQuotes(columns[pos]);
    }

    public static  Map<Integer, String> sortKV(JavaRDD<String> file) {
        JavaPairRDD<Integer, String> kv = file.mapToPair(
                s -> new Tuple2<>(
                        Integer.parseInt(getValue(s, COLUMN_AIRPORT_CODE)),
                        getValue(s, COLUMN_AIRPORT_DESCRIPTION)
                )
        );

        Map<Integer, String> airportsMap = kv.collectAsMap();

        return airportsMap;
    }

}
