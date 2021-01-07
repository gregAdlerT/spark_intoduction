package taxi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Greg Adler
 */
class TaxiServiceTest {
    private static SparkConf conf;
    private static JavaRDD<String> rddDrivers;
    private static JavaRDD<String> rddOrders;
    private static JavaSparkContext sparkContext;
    private TaxiService textService;

    @BeforeAll
    public static void setUpClass(){
        conf = new SparkConf().setAppName("spark_introduction_taxi").setMaster("local[*]");
        sparkContext = new JavaSparkContext(conf);
        rddDrivers = sparkContext.textFile("./drivers.txt");
        rddOrders = sparkContext.textFile("./orders.txt");
    }

    @AfterAll
    public static void onDestroy(){
        sparkContext.close();
    }

    @BeforeEach
    public void setUpMethod(){
        textService = new TaxiService();
    }

    @Test
    void getOrdersCountByCityXAndDistanceGreaterThanY() {
        long res = 
                textService.getOrdersCountByCityXAndDistanceGreaterThanY("Boston", 10, rddOrders);
        Assert.assertTrue(res>0);
        System.out.println(res);
    }

    @Test
    void getTotalDistanceByCity() {
        long res =
                textService.getTotalDistanceByCity("Boston",rddOrders);
        Assert.assertTrue(res>0);
        System.out.println(res);
    }

    @Test
    void getTopXDrivers() {

        long start = Instant.now().toEpochMilli();
        List<String> topXDrivers = textService.getTopXDrivers(rddOrders, rddDrivers, 3);
        long end = Instant.now().toEpochMilli();
        Assert.assertEquals(3,topXDrivers.size());
        System.out.println(end-start);
    }
    
    @Test
    void getTopXDriversByAgeRange(){
        List<Tuple2<Driver,Integer>> topXDriversByAgeRange = 
                textService.getTopXDriversByAgeRange(rddOrders, rddDrivers, 5, 18, 100);
        for (Tuple2 driver : topXDriversByAgeRange) {
            System.out.println(driver);
        };
    }
}