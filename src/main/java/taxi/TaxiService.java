package taxi;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

/**
 * @author Greg Adler
 */
public class TaxiService {
    
    public long getOrdersCountByCityXAndDistanceGreaterThanY(String city, int minDistance, JavaRDD<String>orders){
        return orders.map(l->l.split(", "))
                .filter(o->o[2].trim().equalsIgnoreCase(city.trim()) && Integer.parseInt(o[3].trim())>=minDistance)
                .count();
    }
    
    public int getTotalDistanceByCity(String city, JavaRDD<String>orders){
        return orders.map(l->l.split(", "))
                .filter(o->o[2].trim().equalsIgnoreCase(city.trim()))
                .map(o->Integer.parseInt(o[3]))
                .reduce(Integer::sum);
    }
    
    public List<String> getTopXDrivers(JavaRDD<String>orders, JavaRDD<String>drivers, int amountDriversToReturn){
        JavaPairRDD<String, Integer> driversDistance = orders.coalesce(10)
                .map(l -> l.split(", "))
                .mapToPair(o -> new Tuple2<>(o[1].trim(), Integer.parseInt(o[3])))
                .reduceByKey(Integer::sum);

        JavaPairRDD<String, String> driverName = drivers.map(l -> l.split(", "))
                .mapToPair(d -> new Tuple2<>(d[0].substring(3).trim(), d[1]));


        return driverName.join(driversDistance)
                .map(t->t._2)
                .sortBy(t->t._2,false,8)
                .map(t->t._1)
                .take(amountDriversToReturn);
    }
    
    public List<Tuple2<Driver,Integer>> getTopXDriversByAgeRange(JavaRDD<String>orders,
                                            JavaRDD<String>drivers,
                                            int amountDriversToReturn, int fromAge, int toAge){
        JavaPairRDD<String, Integer> driversDistance = orders.coalesce(10)
                .map(l -> l.split(", "))
                .mapToPair(o -> new Tuple2<>(o[1].trim(), Integer.parseInt(o[3])))
                .reduceByKey(Integer::sum);

        LocalDate now = LocalDate.now();
        JavaPairRDD<String, Driver> driverName = drivers.map(l -> l.split(", "))
                .filter(d-> {
                    long between = (int)ChronoUnit.YEARS.between(LocalDate.parse(d[2].trim()),now);
                    return between>=fromAge && between<=toAge;})
                .mapToPair(d -> new Tuple2<>(d[0].substring(3).trim(),
                        new Driver(Integer.parseInt(d[0].substring(3).trim()),d[1], LocalDate.parse(d[2].trim()))));
//driverName.foreach(System.out::println);

        return driverName.join(driversDistance)
                .map(t->t._2)
                .sortBy(t->t._2,false,8)
              
                .take(amountDriversToReturn);
        
    }
}
