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
    private static final int COLUMN_DELAY = 17;
    private static final int COLUMN_CANCELLED = 19;
    private static final int COLUMN_FLIGHT_TOOK_PLACE = 14;

    private static final int COLUMN_CANCELLED_ERROR_CODE = 20;


    private static final int DELAY_POS_INFO = 0;
    private static final int CANCELLED_POS_INFO = 1;
    private static final int NUBMER_OF_FLIGHTS_POS_INFO = 2;

    private static final float IS_CANCELLED = 1;
    private static final float CANCELLED = 1;
    private static final float NO_CANCELLED = 0;
    private static final float NULL_TIME = 0;
    private static final float ONE_FLIGHT = 1;


    public static String deleteQuotes(String s) {
        return s.replaceAll(QUOTES, EMPTY);
    }

    public static String getValue(String s, int pos) {
        String[] columns = s.split(COMMA, COUNT_AIRPORT_COLUMNS);
        return deleteQuotes(columns[pos]);
    }

    public static JavaPairRDD<Pair<Integer, Integer>, float[]> flightsFromTo(JavaRDD<String> file) {
        return file.mapToPair(
                s -> {
                    Pair<Integer, Integer> airportIDs = new Pair<>(
                            Integer.parseInt(getValue(s, COLUMN_FLIGHT_AIRPORT_ID_FROM)),
                            Integer.parseInt(getValue(s, COLUMN_FLIGHT_AIRPORT_ID_TO))
                    );

                    //[0] время опоздания
                    //[1] отменен рейс или нет
                    //[2] количество рейсов
                    float[] flightsInfo = new float[3];

                    if (getValue(s, COLUMN_CANCELLED_ERROR_CODE).length() > 0) {
                        flightsInfo[CANCELLED_POS_INFO] = NO_CANCELLED;
                        flightsInfo[DELAY_POS_INFO] = Float.parseFloat(getValue(s, COLUMN_DELAY));
                    } else {
                        flightsInfo[CANCELLED_POS_INFO] = CANCELLED;
                        flightsInfo[DELAY_POS_INFO] = NULL_TIME;
                    }

                    flightsInfo[NUBMER_OF_FLIGHTS_POS_INFO] = ONE_FLIGHT;


                    return new Tuple2<>(airportIDs, flightsInfo);
                });
    }

}
