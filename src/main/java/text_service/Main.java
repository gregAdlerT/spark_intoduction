package text_service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

/**
 * @author Greg Adler
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark_introduction").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sparkContext.textFile("D:\\DOWNLOADS\\documents\\warAndPeace.txt");
       // RDD<String>scala_rdd=sparkContext.sc().textFile("D:\\DOWNLOADS\\documents\\warAndPeace",12);
        
        
        
    }
}
