package bmstu.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {





    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = AirportsInfo.loadData(sc, args[0]);



//        JavaRDD<String> airportsKV = airportsWithoutTitle.mapToPair(
//                s -> new Tuple2<>(
//                        Integer.parseInt(airportsWithoutTitle.flatMap(x -> Arrays.stream(x.split(COMMA, 2)))
//                )
//        );

//        JavaRDD<String> check = out.flatMap(x -> Arrays.stream(x.split(COMMA, 2)).iterator());
//        check.saveAsTextFile("output6");



//        JavaRDD<String> delays = sc.textFile(args[1]);
//        delays.saveAsTextFile("output2");


    }
}
