package com.psddev.dari.db;

import org.joda.time.DateTime;

public interface FieldRecalculationDelay {

    public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime);

    public class Minute implements FieldRecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(1).isAfter(lastRunTime);
        }

    }

    public class QuarterHour implements FieldRecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(15).isAfter(lastRunTime);
        }

    }

    public class HalfHour implements FieldRecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(30).isAfter(lastRunTime);
        }

    }

    public class Hour implements FieldRecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusHours(1).isAfter(lastRunTime);
        }

    }

    public class HalfDay implements FieldRecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusHours(12).isAfter(lastRunTime);
        }

    }

    public class Day implements FieldRecalculationDelay {
        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusDays(1).isAfter(lastRunTime);
        }

    }

}
