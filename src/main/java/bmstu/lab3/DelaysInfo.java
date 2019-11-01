package bmstu.lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class DelaysInfo {

    private final static String COMMA = ",";
    private final static String QUOTES = "\"";
    private final static String EMPTY = "";
    private static final int COLUMN_AIRPORT_CODE = 0;
    private static final int COLUMN_AIRPORT_DESCRIPTION = 1;
    private static final int COUNT_AIRPORT_COLUMNS = 2;


    public static String deleteQuotes(String s) {
        return s.replaceAll(QUOTES, EMPTY);
    }

    public static String getValue(String s, int pos) {
        String[] columns = s.split(COMMA, COUNT_AIRPORT_COLUMNS);
        return deleteQuotes(columns[pos]);
    }
}
