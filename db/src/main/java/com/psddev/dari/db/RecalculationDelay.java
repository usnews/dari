package com.psddev.dari.db;

import org.joda.time.DateTime;

public interface RecalculationDelay {

    public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime);

    public class Minute implements RecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(1).isAfter(lastRunTime);
        }

    }

    public class QuarterHour implements RecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(15).isAfter(lastRunTime);
        }

    }

    public class HalfHour implements RecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(30).isAfter(lastRunTime);
        }

    }

    public class Hour implements RecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusHours(1).isAfter(lastRunTime);
        }

    }

    public class HalfDay implements RecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusHours(12).isAfter(lastRunTime);
        }

    }

    public class Day implements RecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusDays(1).isAfter(lastRunTime);
        }

    }

}
