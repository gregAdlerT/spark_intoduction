package text_service;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author Greg Adler
 */
public class TextService {
    
    public long countWords(JavaRDD<String>lines){
        return lines
                .flatMap(l->Arrays.asList(l.split(" ")).iterator())
                .count();
    }
    
    public List<Tuple2<Long, String>> getTop_Words(JavaRDD<String>lines,int amount){
        List<String> notActualWords = List.of("that", "with", "from", "were", "what", "they", "this", "which", "have"
        ,"when","them","there","their","will","been","only","then","himself","into","said","your","after","before");
        return lines
                .map(l->l.replaceAll("[^a-zA-Z\\s]",""))
                .flatMap(l->Arrays.asList(l.split(" ")).iterator())
                .filter(w->w.trim().length()>3 && !notActualWords.contains(w))
                .mapToPair(s -> new Tuple2<String,Long>(s.toLowerCase(),1L))
                .reduceByKey(Long::sum)
                .mapToPair((p)->new Tuple2<>(p._2,p._1))
                .sortByKey(false)
                .take(amount);
    }
}
