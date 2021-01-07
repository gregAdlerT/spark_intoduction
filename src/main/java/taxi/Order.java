package taxi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;

/**
 * @author Greg Adler
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order implements Serializable {
    private long id;
    private int driverId;
    private String city;
    private int distance;
    private LocalDate date;

    @Override
    public String toString() {
        return id+", "+driverId+", "+city+", "+distance+", "+date+"\n";
    }
}
