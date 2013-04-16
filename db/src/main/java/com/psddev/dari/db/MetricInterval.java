package com.psddev.dari.db;

import org.joda.time.DateTime;

// TODO: getSqlDateFormat() needs to support multiple vendors, this is MySQL only.
public interface MetricInterval {

    public long process(DateTime timestamp);
    public String getSqlDateFormat(SqlVendor vendor);

    public class None implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return 0;
        }
        public String getSqlDateFormat(SqlVendor vendor) {
            return "";
        }
    }

    public class Hourly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.hourOfDay().roundFloorCopy().getMillis();
        }
        public String getSqlDateFormat(SqlVendor vendor) {
            return "%Y%m%d%H";
        }
    }

    public class Daily implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.dayOfMonth().roundFloorCopy().getMillis();
        }
        public String getSqlDateFormat(SqlVendor vendor) {
            return "%Y%m%d";
        }
    }

    public class Weekly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.weekOfWeekyear().roundFloorCopy().getMillis();
        }
        public String getSqlDateFormat(SqlVendor vendor) {
            return "%Y%u";
        }
    }

    public class Monthly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.monthOfYear().roundFloorCopy().getMillis();
        }
        public String getSqlDateFormat(SqlVendor vendor) {
            return "%Y%m";
        }
    }

    public class Yearly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.year().roundFloorCopy().getMillis();
        }
        public String getSqlDateFormat(SqlVendor vendor) {
            return "%Y";
        }
    }
}
