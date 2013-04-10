package com.psddev.dari.db;

import org.joda.time.DateTime;

public interface MetricInterval {

    public long process(DateTime timestamp);

    public class None implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return 0;
        }
    }

    public class Hourly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.hourOfDay().roundFloorCopy().getMillis();
        }
    }

    public class Daily implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.dayOfMonth().roundFloorCopy().getMillis();
        }
    }

    public class Weekly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.weekOfWeekyear().roundFloorCopy().getMillis();
        }
    }

    public class Monthly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.monthOfYear().roundFloorCopy().getMillis();
        }
    }

    public class Yearly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.year().roundFloorCopy().getMillis();
        }
    }
}
