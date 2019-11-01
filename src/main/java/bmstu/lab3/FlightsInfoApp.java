package bmstu.lab3;

import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;


public class FlightsInfoApp {

    public static JavaRDD<String> loadData(JavaSparkContext sc, String path) {
        JavaRDD<String> text = sc.textFile(path);
        String title = text.first();
        return text.filter(s -> !s.equals(title));
    }

    public static JavaRDD<String> allDataToString(JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo,
                                                  Broadcast<Map<Integer, String>> airportsInfo) {
        return delaysInfo.map(
                info -> {
                    int firstAirportID = info._1.getKey();
                    int secondAirportID = info._1.getValue();
                    String data = info._2;

                    String firstAirportName = airportsInfo.getValue().get(firstAirportID);
                    String secondAirportName = airportsInfo.getValue().get(secondAirportID);

                    return "From (" + firstAirportID + "   " + firstAirportName + ")"
                            + " to (" + secondAirportID + "   " + secondAirportName + ") "
                            + data;
                }
        );
    }


    public static void main(String[] args) {

        if (args.length != 3) {
            System.err.println("Usage: FlightsInfoApp <airports file path> <delays file path> <output path>");
            System.exit(-1);
        }

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = loadData(sc, args[0]);
        JavaRDD<String> delays = loadData(sc, args[1]);


        Map<Integer, String> airportsInfo = AirportsInfo.sortKV(airports);
        JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo = DelaysInfo.sort(delays);

        Broadcast<Map<Integer, String>> broadcast = sc.broadcast(airportsInfo);

        JavaRDD<String> outInfo = allDataToString(delaysInfo, broadcast);

        outInfo.saveAsTextFile(args[2]);


    }
}
