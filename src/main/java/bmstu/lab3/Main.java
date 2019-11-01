package bmstu.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class Main {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = AirportsInfo.loadData(sc, args[0]);

        JavaPairRDD<Integer, String> aiportsKV = AirportsInfo.sortKV(airports);

        aiportsKV.saveAsTextFile("ouput7");




    }
}
