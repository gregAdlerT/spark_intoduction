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
public class Driver implements Serializable {
    private int id;
    private String name;
    private LocalDate birthday;

    @Override
    public String toString() {
        return  id+", "+name+", "+birthday+"\n";
    }
}
