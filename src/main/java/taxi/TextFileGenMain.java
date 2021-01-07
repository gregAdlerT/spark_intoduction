package taxi;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.javafaker.Faker;
import lombok.SneakyThrows;

import java.io.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Greg Adler
 */
public class TextFileGenMain {
    private static final int ORDERS_AMOUNT = 1000000;
    private static final int DRIVERS_AMOUNT = 30;
    private static Random random= new Random();
    

    public static void main(String[] args) {
        List<Driver>drivers= generateDrivers();
        List<Order>orders= generateOrders(drivers);
        writeToFile(drivers,"./drivers.txt");
        writeToFile(orders,"./orders.txt");
        
//        read("./drivers.txt",Driver.class);
    }

//    @SneakyThrows
//    private static void read(String path, Class<?>tClass) {
//        Kryo kryo = new Kryo();
//        try (Input input= new Input(new DataInputStream(new FileInputStream(new File(path))))){
//            Object object = kryo.readObject(input, tClass);
//            System.out.println(object);
//        }
//    }
//

    @SneakyThrows
    private static void writeToFile(List<?> list, String path) {
       // Kryo kryo = new Kryo();
        try (ObjectOutputStream output= new ObjectOutputStream(new FileOutputStream(new File(path)))){
            int count=0;
            for (Object obj : list) {
                output.writeObject(obj.toString());
//                kryo.writeObject(output,obj.toString());
            }
        }
    }

    private static List<Driver> generateDrivers() {
        Faker faker = new Faker();
        return random.ints(1,Integer.MAX_VALUE)
                .distinct()
                .limit(TextFileGenMain.DRIVERS_AMOUNT)
                .mapToObj(i->{
                    String firstName = faker.name().firstName();
                    String secondName = faker.name().lastName();
                    LocalDate date = 
                            LocalDate.ofInstant(faker.date().birthday(19,90).toInstant(), ZoneId.systemDefault());
                    return new Driver(i,firstName+" "+secondName,date);})
                .collect(Collectors.toList());
    }

    private static List<Order> generateOrders(List<Driver> drivers) {
        Faker faker = new Faker();
        return IntStream.range(1, TextFileGenMain.ORDERS_AMOUNT +1).mapToObj(i->{
            String city = faker.address().cityName();
            int distance = random.nextInt(100);
            LocalDate date =
                    LocalDate.ofInstant(
                            faker.date().past(30, TimeUnit.DAYS).toInstant(), ZoneId.systemDefault());
            return new Order(i,drivers.get(random.nextInt(drivers.size())).getId(),city,distance,date);})
                .collect(Collectors.toList());
        
    }
}
