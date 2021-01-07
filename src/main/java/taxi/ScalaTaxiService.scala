package taxi

import org.apache.spark.rdd.RDD

/**
 * @author Greg Adler
 */
class ScalaTaxiService {
  
  def getOrdersCountByCityXAndDistanceGreaterThanY(city:String, minDistance:Int, orders:RDD[String]):Long={
    orders.map(_.split(", "))
      .filter(a=>a(2).trim.equalsIgnoreCase(city) && a(3).trim.toInt==minDistance)
      .count()
  }
  
  def getTotalDistanceByCity(city:String, orders: RDD[String]):Int={
    orders.map(_.split(", "))
      .filter(_(2).trim.equalsIgnoreCase(city))
      .map(_(3).trim.toInt)
      .reduce(_+_)
  }
  
  def getTopXDrivers(orders: RDD[String],drivers: RDD[String],amountDriversToReturn:Int):List[String]={
    orders.map(_.split(", "))
      .groupBy(_(1))
      .map(t=>(t._1,t._2.map(a=>a(3).trim.toInt).sum))
      .join(drivers
        .map(_.split(", "))
        .groupBy(a=>a(0).substring(3)))
      .sortBy(_._2._1,ascending = false,numPartitions = 8)
      .map(_._2._2.toList.head(1))
      .take(amountDriversToReturn)
      .toList
  }

}
