package bmstu.lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class DelaysInfo {

    private final static String COMMA = ",";
    private final static String QUOTES = "\"";
    private final static String EMPTY = "";

    private static final int COUNT_AIRPORT_COLUMNS = 23;
    private static final int COLUMN_AIRPORT_ID_FROM = 11;
    private static final int COLUMN_AIRPORT_ID_TO = 14;


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
                        Integer.parseInt(getValue(s, COLUMN_AIRPORT_ID_FROM)),
                        Integer.parseInt(getValue(s, COLUMN_AIRPORT_ID_TO))
                )
        );

        System.out.println("testGit");

        return kv;
    }

}
