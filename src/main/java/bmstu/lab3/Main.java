package bmstu.lab3;

import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.List;
import java.util.Map;


public class Main {

    public static JavaRDD<String> loadData(JavaSparkContext sc, String path) {
        JavaRDD<String> text = sc.textFile(path);
        String title = text.first();
        return text.filter(s -> !s.equals(title));
    }

    public static JavaRDD<String> allDataToString(JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo) {
        
    }


    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = loadData(sc, args[0]);
        JavaRDD<String> delays = loadData(sc, args[1]);


        Map<Integer, String> airportsInfo = AirportsInfo.sortKV(airports);
        JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo = DelaysInfo.sort(delays);

        Broadcast<Map<Integer, String>> broadcast = sc.broadcast(airportsInfo);




    }
}
