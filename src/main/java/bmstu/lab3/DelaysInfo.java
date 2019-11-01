package bmstu.lab3;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class DelaysInfo {

    private final static String COMMA = ",";

    private static final int COUNT_AIRPORT_COLUMNS = 23;
    private static final int COLUMN_FLIGHT_AIRPORT_ID_FROM = 11;
    private static final int COLUMN_FLIGHT_AIRPORT_ID_TO = 14;
    private static final int COLUMN_DELAY = 17;

    private static final int DELAY_POS_INFO = 0;
    private static final int CANCELLED_POS_INFO = 1;
    private static final int NUBMER_OF_FLIGHTS_POS_INFO = 2;
    private static final int COUNT_OF_FLIGHTS_POS_INFO = 3;
    private static final int COUNT_OF_CANCELLED_POS_INFO = 4;
    private static final int MAX_DELAY_POS_INFO = 5;
    private static final int COUNT_OF_DELAY_FLIGHTS_POS_INFO = 6;

    private static final float CANCELLED = 1;
    private static final float NO_CANCELLED = 0;
    private static final float NULL_TIME = 0;
    private static final float ONE_FLIGHT = 1;

    public static JavaPairRDD<Pair<Integer, Integer>, String> sort(JavaRDD<String> file) {
        JavaPairRDD<Pair<Integer, Integer>, float[]> mapFile = flightsFromTo(file);

        JavaPairRDD<Pair<Integer, Integer>, float[]> reduceFile = reduce(mapFile);

        return toString(reduceFile);
    }



    public static String getValue(String s, int pos) {
        String[] columns = s.split(COMMA, COUNT_AIRPORT_COLUMNS);
        return columns[pos];
    }

    public static JavaPairRDD<Pair<Integer, Integer>, float[]> flightsFromTo(JavaRDD<String> file) {
        return file.mapToPair(
                s -> {
                    Pair<Integer, Integer> airportIDs = new Pair<>(
                            Integer.parseInt(getValue(s, COLUMN_FLIGHT_AIRPORT_ID_FROM)),
                            Integer.parseInt(getValue(s, COLUMN_FLIGHT_AIRPORT_ID_TO))
                    );

                    float[] flightsInfo = new float[7];

                    if (getValue(s, COLUMN_DELAY).length() > 0) {
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

    public static JavaPairRDD<Pair<Integer, Integer>, float[]> reduce(JavaPairRDD<Pair<Integer, Integer>, float[]> info) {
        return info.reduceByKey(
                (info1, info2) -> {

                    info1[COUNT_OF_FLIGHTS_POS_INFO] += info2[COUNT_OF_FLIGHTS_POS_INFO];

                    info1[COUNT_OF_CANCELLED_POS_INFO] += info2[COUNT_OF_CANCELLED_POS_INFO] + info1[CANCELLED_POS_INFO] + info2[CANCELLED_POS_INFO];

                    info1[MAX_DELAY_POS_INFO] = Float.max(info1[MAX_DELAY_POS_INFO], info2[MAX_DELAY_POS_INFO]);

                    info1[COUNT_OF_DELAY_FLIGHTS_POS_INFO] += info2[COUNT_OF_DELAY_FLIGHTS_POS_INFO];

                    return info1;
                }
        );
    }

    public static JavaPairRDD<Pair<Integer, Integer>, String> toString(JavaPairRDD<Pair<Integer, Integer>, float[]> info) {
        return info.mapValues(
                data -> "Maximum delay time: " + data[MAX_DELAY_POS_INFO]
                        + "    Percent delays: " + (data[COUNT_OF_DELAY_FLIGHTS_POS_INFO] / data[COUNT_OF_FLIGHTS_POS_INFO] * 100) + "%"
                        + "    Percent cancelled: " + (data[COUNT_OF_CANCELLED_POS_INFO] / data[COUNT_OF_FLIGHTS_POS_INFO] * 100) + "%"
        );
    }



}
