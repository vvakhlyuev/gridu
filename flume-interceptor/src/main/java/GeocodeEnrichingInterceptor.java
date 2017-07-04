import org.apache.commons.net.util.SubnetUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by vvakhlyuev on 29/06/2017.
 */
public class GeocodeEnrichingInterceptor extends AbstractFlumeInterceptor implements Interceptor {
    public static final Logger LOGGER = LoggerFactory.getLogger(GeocodeEnrichingInterceptor.class);
    private static final List<String> masks = Arrays.asList("255.255.255.255", "255.255.255.254", "255.255.255.252", "255.255.255.248", "255.255.255.240", "255.255.255.224", "255.255.255.192", "255.255.255.128", "255.255.255.0", "255.255.254.0", "255.255.252.0", "255.255.248.0", "255.255.240.0", "255.255.224.0", "255.255.192.0", "255.255.128.0", "255.255.0.0", "255.254.0.0", "255.252.0.0", "255.248.0.0", "255.240.0.0", "255.224.0.0", "255.192.0.0", "255.128.0.0", "255.0.0.0");
    private final Pattern ipAddressPattern = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");

    private Map<Long, String> lowIps = new HashMap<Long, String>();
    private String geoLite2Path;

    private GeocodeEnrichingInterceptor(String geoLite2Path) {
        this.geoLite2Path = geoLite2Path;
    }

    public void initialize() {
        String line;
        BufferedReader br = null;
        int countLines = 0;
        try {
            br = new BufferedReader(new FileReader(geoLite2Path));
            while ((line = br.readLine()) != null) {
                if (countLines < 1) {
                    countLines++;
                    continue;
                }
                String[] split = line.split(",");
                String network = split[0];
                String geoname_id = split[1];
                // Problem with these masks
                if (network.endsWith("/31") || network.endsWith("/32")) {
                    continue;
                }

                SubnetUtils utils = new SubnetUtils(network);
                String lowAddress = utils.getInfo().getLowAddress();
                long lowAddressAsLong = IPToLong(lowAddress);
                lowIps.put(lowAddressAsLong, geoname_id);
            }
            LOGGER.info("Populated records of " + lowIps.size() + " CIDR ranges");
        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public Event intercept(Event event) {
        String line = new String(event.getBody());
        String[] split = line.split(",");
        String ipAddress = split[4];

        if (!ipAddressPattern.matcher(ipAddress).matches()) {
            LOGGER.error("Skipping event, not an IP: " + line);
            return null;
        }

        try {
            for (String m : masks) {
                SubnetUtils utils = new SubnetUtils(ipAddress, m);
                String lowAddressString = utils.getInfo().getLowAddress();
                if (isSingleAddress(m)) {
                    lowAddressString = ipAddress;
                }
                long lowIp = IPToLong(lowAddressString);
                if (lowIps.containsKey(lowIp)) {
                    line = line + "," + lowIps.get(lowIp);
                    event.setBody(line.getBytes());
                    LOGGER.debug("PROCESSED " + new String(event.getBody()));
                    return event;
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
        LOGGER.info("Couldn't find IP address for event: " + event);
        return null;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        private String path;

        public void configure(Context context) {
            this.path = context.getString(Constants.PATH, ".");
        }

        public Interceptor build() {
            LOGGER.info(String.format("Creating GeocodeEnrichingInterceptor: geoLite2Path=%s", path));
            return new GeocodeEnrichingInterceptor(path);
        }
    }

    public static class Constants {
        public static final String PATH = "geoLite2Path";
    }

    private boolean isSingleAddress(String m) {
        return m.equals("255.255.255.255") || m.equals("255.255.255.254");
    }

    public long IPToLong(String addr) {
        String[] addrArray = addr.split("\\.");
        long num = 0;
        for (int i = 0; i < addrArray.length; i++) {
            int power = 3 - i;
            num += ((Integer.parseInt(addrArray[i]) % 256 * Math.pow(256, power)));
        }
        return num;
    }

    public String longToIP(long ip) {
        return ((ip >> 24) & 0xFF) + "."
                + ((ip >> 16) & 0xFF) + "."
                + ((ip >> 8) & 0xFF) + "."
                + (ip & 0xFF);

    }
}
