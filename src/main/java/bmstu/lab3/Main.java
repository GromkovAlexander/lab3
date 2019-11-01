package bmstu.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile(args[0]);
        airports.flatMap(x -> x.split(","));

        airports.flatMap(x -> Arrays.stream(x.split(",")).iterator());
        airports.saveAsTextFile("output1");



        JavaRDD<String> delays = sc.textFile(args[1]);
        delays.saveAsTextFile("output2");


    }
}
