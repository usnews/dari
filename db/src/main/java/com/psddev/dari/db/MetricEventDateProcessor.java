package com.psddev.dari.db;

// TODO: Calendar is slow - use org.joda.time instead
import java.util.Calendar;

public interface MetricEventDateProcessor {

    public long process(long timestampMillis);

    public class None implements MetricEventDateProcessor {
        public long process(long timestampMillis) {
            return 0;
        }
    }

    public class Hourly implements MetricEventDateProcessor {
        public long process(long timestampMillis) {
            Calendar c = Calendar.getInstance();
            c.clear();
            c.setTimeInMillis(timestampMillis);
            c.set(Calendar.MILLISECOND, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MINUTE, 0);
            return c.getTimeInMillis();
        }
    }

    public class Daily implements MetricEventDateProcessor {
        public long process(long timestampMillis) {
            Calendar c = Calendar.getInstance();
            c.clear();
            c.setTimeInMillis(timestampMillis);
            c.set(Calendar.MILLISECOND, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.HOUR_OF_DAY, 0);
            return c.getTimeInMillis();
        }
    }

    public class Weekly implements MetricEventDateProcessor {
        public long process(long timestampMillis) {
            Calendar c = Calendar.getInstance();
            c.clear();
            c.setTimeInMillis(timestampMillis);
            c.set(Calendar.MILLISECOND, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.HOUR_OF_DAY, 0);
            while (c.get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY)
                c.add(Calendar.DATE, -1);
            return c.getTimeInMillis();
        }
    }

    public class Monthly implements MetricEventDateProcessor {
        public long process(long timestampMillis) {
            Calendar c = Calendar.getInstance();
            c.clear();
            c.setTimeInMillis(timestampMillis);
            c.set(Calendar.MILLISECOND, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.DAY_OF_MONTH, 1);
            return c.getTimeInMillis();
        }
    }

    public class Yearly implements MetricEventDateProcessor {
        public long process(long timestampMillis) {
            Calendar c = Calendar.getInstance();
            c.clear();
            c.setTimeInMillis(timestampMillis);
            c.set(Calendar.MILLISECOND, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.DAY_OF_MONTH, 1);
            c.set(Calendar.MONTH, Calendar.JANUARY);
            return c.getTimeInMillis();
        }
    }

}

