import org.apache.commons.net.util.SubnetUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by vvakhlyuev on 03/07/2017.
 */
class Helper {
    private final Logger LOGGER = Logger.getLogger(Helper.class);
    private DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private Random random = new Random();
    private RandomGaussian priceGaussian = new RandomGaussian(500, 200, 0.1, 1000, 2);
    private RandomGaussian hoursGaussian = new RandomGaussian(12, 4, 0, 23);
    private RandomGaussian minSecGaussian = new RandomGaussian(30, 10, 0, 59);

    private String cidrFilePath;
    private List<String> cidrs = new ArrayList<String>();

    private DateTime startDate;
    private DateTime endDate;

    public Helper(DateTime startDate, DateTime endDate, String cidrFilePath) {
        this.cidrFilePath = cidrFilePath;
        this.startDate = startDate;
        this.endDate = endDate;
        loadCidrData();
    }

    private void loadCidrData() {
        LOGGER.info("Loading CIDR records from " + cidrFilePath);
        String line;
        BufferedReader br = null;
        int countLines = 0;
        try {
            br = new BufferedReader(new FileReader(cidrFilePath));
            while ((line = br.readLine()) != null) {
                if (countLines < 1) {
                    countLines++;
                    continue;
                }
                String[] split = line.split(",");
                String cidr = split[0];
                cidrs.add(cidr);
            }
            LOGGER.info("Populated records of " + cidrs.size() + " CIDR ranges");
        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private String generateIP() {
        int lineNumber = random.nextInt(cidrs.size());
        String cidr = cidrs.get(lineNumber).trim();
        // Problem with theses masks
        while (cidr.endsWith("/31") || cidr.endsWith("/32")) {
            cidr = cidrs.get(this.random.nextInt(cidrs.size()));
        }
        SubnetUtils util = new SubnetUtils(cidr);
        String[] ipAddresses = util.getInfo().getAllAddresses();
        int index = random.nextInt(ipAddresses.length);
        String ipAddress = ipAddresses[index];
        LOGGER.trace("Selected " + ipAddress + " from " + cidr);
        return ipAddress;
    }

    private <T> T randomValueFromList(List<T> list) {
        int size = list.size();
        return list.get(random.nextInt(size));
    }

    private String randomDate() {
        int days = Days.daysBetween(startDate, endDate).getDays();
        DateTime date = startDate.plusDays(random.nextInt(days));

        date = date.plusHours(hoursGaussian.getGaussianInt());
        date = date.plusMinutes(minSecGaussian.getGaussianInt());
        date = date.plusSeconds(minSecGaussian.getGaussianInt());

        return dtf.print(date);
    }

    public String newRandomPurchase() {
        String name = randomValueFromList(Constants.products);
        double price = priceGaussian.getGaussianDouble();
        String date = randomDate();
        String category = randomValueFromList(Constants.categories);
        String ip = generateIP();

        StringBuilder sb = new StringBuilder(name);
        sb.append(",").append(price).append(",").append(date).append(",")
                .append(category).append(",").append(ip);
        return sb.toString();
    }

}
