package com.psddev.dari.db;

import org.joda.time.DateTime;

public interface IndexUpdateDelay {

    public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime);
    public boolean isImmediate();

    public class Immediate implements IndexUpdateDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return true;
        }

        @Override
        public boolean isImmediate() {
            return true;
        }
    }

    public class Minutely implements IndexUpdateDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(1).isAfter(lastRunTime);
        }

        @Override
        public boolean isImmediate() {
            return false;
        }
    }

    public class QuarterHourly implements IndexUpdateDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(15).isAfter(lastRunTime);
        }

        @Override
        public boolean isImmediate() {
            return false;
        }
    }

    public class HalfHourly implements IndexUpdateDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(30).isAfter(lastRunTime);
        }

        @Override
        public boolean isImmediate() {
            return false;
        }
    }

    public class Hourly implements IndexUpdateDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusHours(1).isAfter(lastRunTime);
        }

        @Override
        public boolean isImmediate() {
            return false;
        }
    }

    public class HalfDaily implements IndexUpdateDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusHours(12).isAfter(lastRunTime);
        }

        @Override
        public boolean isImmediate() {
            return false;
        }
    }

    public class Daily implements IndexUpdateDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusDays(1).isAfter(lastRunTime);
        }

        @Override
        public boolean isImmediate() {
            return false;
        }
    }

}
