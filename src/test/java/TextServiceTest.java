import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import scala.Tuple2;
import text_service.ScalaTextService;
import text_service.TextService;

import java.util.List;

/**
 * @author Greg Adler
 */
class TextServiceTest {
    public static SparkConf conf;
    public static JavaRDD<String> rdd;
    public static JavaSparkContext sparkContext;
    private TextService textService;
    
    @BeforeAll
    public static void setUpClass(){
        conf = new SparkConf().setAppName("spark_introduction").setMaster("local[*]");
        sparkContext = new JavaSparkContext(conf);
        rdd = sparkContext.textFile("D:\\DOWNLOADS\\documents\\warAndPeace.txt");
    }
    
    @AfterAll
    public static void onDestroy(){
        sparkContext.close();
    }
    
    @BeforeEach
    public void setUpMethod(){
        textService = new TextService();
    }


    @org.junit.jupiter.api.Test
    void countWords() {
        long res = textService.countWords(rdd);
        System.out.println(res);
        Assert.assertTrue(res>0);
        
        

        JavaRDD<String> javaRDD = sparkContext.parallelize(List.of("java", "scala", "python"));
        long actual = textService.countWords(javaRDD);
        Assert.assertEquals(3,actual);
    }

    @org.junit.jupiter.api.Test
    void getTop_Words(){
        List<Tuple2<Long, String>> javaTopWordsResult = textService.getTop_Words(rdd, 20);
        Assert.assertNotNull(javaTopWordsResult);
        Assert.assertEquals(20,javaTopWordsResult.size());
        System.out.println(javaTopWordsResult);
        System.out.println("---------------------------------------------------");

        ScalaTextService scalaTextService = new ScalaTextService();
        List<Tuple2<String, Object>> scalaTopWordsResult =
                scalaTextService.getTop_Words(rdd.rdd(), 20);
        System.out.println(scalaTopWordsResult);

        for(int i=0;i<javaTopWordsResult.size();i++){
            Assert.assertEquals(javaTopWordsResult.get(i)._2(),scalaTopWordsResult.get(i)._1);
        }
    }
}