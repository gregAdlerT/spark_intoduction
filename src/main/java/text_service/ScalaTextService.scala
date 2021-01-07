package text_service

import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

/**
 * @author Greg Adler
 */
class ScalaTextService {
  def getTop_Words(rdd:RDD[String], amount: Int):java.util.List[Tuple2[String,Long]]={
    val notActualWords = List("that", "with", "from", "were", "what", "they", "this", "which", "have", "when", "them",
      "there", "their", "will", "been", "only", "then", "himself", "into", "said", "your", "after", "before")
    rdd.flatMap(l=>l.replaceAll("[^a-zA-Z\\s]","").split(" "))
      .filter(w=>w.trim.length>3 && !notActualWords.contains(w.trim))
      .map(w=>(w.toLowerCase,1L))
      .reduceByKey(_+_)
      .sortBy(_._2,ascending = false)
      .take(amount)
      .toList asJava
  }
}
