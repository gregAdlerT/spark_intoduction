package text_service

import com.github.javafaker.Faker
import org.apache.spark.SparkContext

/**
 * @author Greg Adler
 */
object ScalaTextMain {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[*]","spark-scala")
    val faker = new Faker()
    
    var list:List[String]=Nil
    (1 to 100000).foreach(_=>list = faker.name().username()::list)
    val rdd = sparkContext.parallelize(list)
    val service = new ScalaTextService
    
    val res = service.getTop_Words(rdd,10)
    println(res)
    sparkContext.stop()
  }
}
