package com.psddev.dari.db;

import org.joda.time.DateTime;

public interface MetricInterval {

    public long process(DateTime timestamp);
    public String getSqlTruncatedDateFormat(SqlVendor vendor);

    public class None implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return 0;
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            return "0";
        }
    }

    public class Hourly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.hourOfDay().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL)
                return "%Y%m%d%H";
            else
                throw new DatabaseException(vendor.getDatabase(), "Metrics is not fully implemented for this vendor");
        }
    }

    public class Daily implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.dayOfMonth().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL)
                return "%Y%m%d";
            else
                throw new DatabaseException(vendor.getDatabase(), "Metrics is not fully implemented for this vendor");
        }
    }

    public class Weekly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.weekOfWeekyear().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL)
                return "%Y%u";
            else
                throw new DatabaseException(vendor.getDatabase(), "Metrics is not fully implemented for this vendor");
        }
    }

    public class Monthly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.monthOfYear().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL)
                return "%Y%m";
            else
                throw new DatabaseException(vendor.getDatabase(), "Metrics is not fully implemented for this vendor");
        }
    }

    public class Yearly implements MetricInterval {
        @Override
        public long process(DateTime timestamp) {
            return timestamp.year().roundFloorCopy().getMillis();
        }
        public String getSqlTruncatedDateFormat(SqlVendor vendor) {
            if (vendor instanceof SqlVendor.MySQL)
                return "%Y";
            else
                throw new DatabaseException(vendor.getDatabase(), "Metrics is not fully implemented for this vendor");
        }
    }
}
