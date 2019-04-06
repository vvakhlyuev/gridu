import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Created by vvakhlyuev on 29/06/2017.
 */
public abstract class AbstractFlumeInterceptor implements Interceptor {
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractFlumeInterceptor.class);

    public List<Event> intercept(List<Event> events) {
        for (Iterator<Event> iterator = events.iterator(); iterator.hasNext(); ) {
            Event next =  intercept(iterator.next());
            if(next == null) {
                LOGGER.info("Got null event, dropping");
                iterator.remove();
            }
        }
        LOGGER.info("Processed " + events.size() + " events");
        return events;
    }
}
