import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Random;

/**
 * Created by vvakhlyuev on 29/06/2017.
 */



public class Generator {

    public static void main(String[] args) throws Exception {
        int numEvents = 1000;
        int sleepBetween = 50;

        DateTime startDate = new DateTime(2017,06,05, 0,0);
        DateTime endDate = new DateTime(2017,06,12, 0,0);

        String cidrFile = "/Users/vvakhlyuev/Projects/gridu/event-generator/src/main/resources/GeoLite2-City-CSV_20170606/GeoLite2-City-Blocks-IPv4.csv";
        Helper h = new Helper(startDate, endDate, cidrFile);

        Socket socket = new Socket(InetAddress.getByName("192.168.56.101"), 9999);
        PrintStream out = new PrintStream(socket.getOutputStream());

        // Prod6, 66, 2017-06-01 01:00, Category6, 1.1.238.37
        // NWOB! Skechers WORK Boots,689.18,2017-06-07 12:25:16.000,Video Games & Consoles,99.244.74.233
        for (int i = 0; i < numEvents; i++) {
            String event = h.newRandomPurchase();
            out.println(event);
//            System.out.println(event);
            out.flush();
            Thread.sleep(sleepBetween);
        }
        out.flush();
        socket.close();
    }
}

