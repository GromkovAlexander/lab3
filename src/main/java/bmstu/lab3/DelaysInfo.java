package bmstu.lab3;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class DelaysInfo {

    private final static String COMMA = ",";
    private final static String QUOTES = "\"";
    private final static String EMPTY = "";

    private static final int COUNT_AIRPORT_COLUMNS = 23;
    private static final int COLUMN_FLIGHT_AIRPORT_ID_FROM = 11;
    private static final int COLUMN_FLIGHT_AIRPORT_ID_TO = 14;
    private static final int COLUMN_DELAY_POS = 19;
    private static final int COLUMN_FLIGHT_TOOK_PLACE = 14;

    private static final int DELAY_POS_INFO = 0;
    private static final int TOOK_PLACE_POS_INFO = 1;
    private static final int NUBMER_OF_FLIGHTS_POS_INFO = 2;




    public static String deleteQuotes(String s) {
        return s.replaceAll(QUOTES, EMPTY);
    }

    public static String getValue(String s, int pos) {
        String[] columns = s.split(COMMA, COUNT_AIRPORT_COLUMNS);
        return deleteQuotes(columns[pos]);
    }

    public static JavaPairRDD<Integer, Integer> flightsFromTo(JavaRDD<String> file) {
        JavaPairRDD<Pair<Integer, Integer>, float[]> kv = file.mapToPair(
                s -> {
                    Pair<Integer, Integer> airportIDs = new Pair<>(
                            Integer.parseInt(getValue(s, COLUMN_FLIGHT_AIRPORT_ID_FROM)),
                            Integer.parseInt(getValue(s, COLUMN_FLIGHT_AIRPORT_ID_TO))
                    );

                    float[] flightsInfo = new float[3];

                    if (Integer.parseInt(getValue(s, COLUMN_DELAY_POS)) == 1) {

                    }


                    return new Tuple2<>()
                });



        //время опоздания
        //отменен рейс или нет
        //количество рейсов

        if (Integer.parseInt(getValue()))






        return kv;
    }

}
