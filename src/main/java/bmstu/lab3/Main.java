package bmstu.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Main {

    private final static String COMMA = ",";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile(args[0]);

        String title = airports.first();

        JavaRDD<String> out = airports.filter(line -> !line.equals(title));

        JavaRDD<String> check = out.flatMap(x -> Arrays.stream(x.split("\t")).iterator());
        check.saveAsTextFile("output5");



//        JavaRDD<String> delays = sc.textFile(args[1]);
//        delays.saveAsTextFile("output2");


    }
}
