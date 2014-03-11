package com.psddev.dari.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** @deprecated Use <a href="http://joda-time.sourceforge.net/">Joda Time</a> instead. */
@Deprecated
public final class DateUtils {

    private static final long DAY_MS = 1000 * 60 * 60 * 24;

    // Specified as: (optional time zone id) date format
    private static final String[] FORMATS = {
            "(GMT)yyyy-MM-dd'T'HH:mm:ss'Z'",
            "(GMT)yyyy-MM-dd'T'HH:mm:ss.S'Z'",
            "EEE MMM dd HH:mm:ss z yyyy",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
            "yyyy-MM-dd'T'HH:mm" };

    /** To make SimpleDateFormat thread-safe. */
    private static final Map<String, DateTimeFormatter>
            FORMATTERS = new PullThroughCache<String, DateTimeFormatter>() {

        @Override
        protected DateTimeFormatter produce(String format) {

            // Has time zone?
            Matcher tzMatcher = StringUtils
                    .getMatcher(format, "^\\(([^)]+)\\)\\s*(.+)$");
            if (tzMatcher.matches()) {
                return DateTimeFormat
                        .forPattern(tzMatcher.group(2))
                        .withZone(DateTimeZone.forID(tzMatcher.group(1)));
            } else {
                return DateTimeFormat.forPattern(format);
            }
        }
    };

    /**
     * Converts the given formatted date string into a date object.
     * @throws DateFormatException
     *         If the given string is null or is not a valid date.
     */
    public static Date fromString(String string, String format) {

        if (string == null) {
            throw new DateFormatException(
                    "Cannot convert a null string into a date!");
        }

        string = string.trim();
        try {
            return new Date(FORMATTERS.get(format).parseMillis(string));
        } catch (IllegalArgumentException error) {
            // Try a different conversion or error below.
        }

        if (format.contains("z")) {
            try {
                return new SimpleDateFormat(format).parse(string);
            } catch (ParseException error) {
                // Error below.
            }
        }

        throw new DateFormatException(String.format(
                "Cannot convert [%s] into a date using the [%s] format!",
                string, format));
    }

    /**
     * Converts the given formatted date string into a date object
     * Each of the predefined formats is checked to see if they can parse
     * the input using both JodaTime and, if that fails, SimpleDateFormat
     *
     * @throws DateFormatException
     *         If the given string is null or is not a valid date.
     */
    public static Date fromString(String string) {

        if (string == null) {
            throw new DateFormatException(
                    "Cannot convert a null string into a date!");
        }

        string = string.trim();
        for (String format : FORMATS) {

            try {
                return new Date(FORMATTERS.get(format).parseMillis(string));
            } catch (IllegalArgumentException error) {
                // Try a different conversion or the next format.
            }

            if (format.contains("z")) {
                try {
                    return new SimpleDateFormat(format).parse(string);
                } catch (ParseException error) {
                    // Try the next format.
                }
            }
        }

        throw new DateFormatException(String.format(
                "[%s] is not a valid date!", string));
    }

    /** Converts the given date into a string using the given format. */
    public static String toString(Date date, String format) {
        return date == null ? null : FORMATTERS.get(format).print(date.getTime());
    }

    /** Converts the given date into a string using the default format. */
    public static String toString(Date date) {
        return toString(date, FORMATS[0]);
    }

    /**
     * Converts the duration between the current datetime and the provided one
     * into an easier-to-read label, such as "5 minutes ago" or "1 hour ago". Durations
     * longer than 1 day are converted to a full, short datetime using the format
     * {@code EEE, MMM d, yyyy, h:mm a zz}
     * @return the pretty printed duration
     */
    public static String toLabel(Date date, boolean isShortened) {
        if (date == null) {
            return null;
        }
        if (isShortened) {
            long diff = (new Date().getTime() - date.getTime()) / 1000;
            if (diff < 60) {
                return "less than a minute ago";
            } else if ((diff /= 60) < 60) {
                return diff == 1 ? "1 minute ago" : diff + " minutes ago";
            } else if ((diff /= 60) < 24) {
                return diff == 1 ? "1 hour ago" : diff + " hours ago";
            }
        }
        return toString(date, "EEE, MMM d, yyyy, h:mm a zz");
    }

    /**
     * Converts the given date object into an easier-to-read label.
     */
    public static String toLabel(Date date) {
        return toLabel(date, true);
    }

    /**
     * Checks to see if two dates are on the same day
     */
    public static boolean isSameDay(Date a, Date b) {
        return a.getTime() / DAY_MS * DAY_MS == b.getTime() / DAY_MS * DAY_MS;
    }

    /**
     * Adds or subtract time from date.
     *
     * @see Calendar#DAY_OF_MONTH
     * @see Calendar#YEAR
     * @see Calendar#MONTH
     * @see Calendar#DAY_OF_WEEK
     */
    public static Date addTime(Date date, int field, int amount) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(field, amount);
        return calendar.getTime();
    }

    /**
     * Converts the duration between the current datetime and the provided one
     * into an easier-to-read label, such as "5 minutes ago", "1 hour ago", or
     * "2 days ago".
     */
    public static String toSimpleElapsedTime(Date date) {
        long diff = (System.currentTimeMillis() - date.getTime()) / 1000;

        if (diff < 60) {
            return "less than a minute ago";
        }
        diff /= 60;
        if (diff < 60) {
            return pluralize(diff, "minute") + " ago";
        }
        diff /= 60;
        if (diff < 24) {
            return pluralize(diff, "hour") + " ago";
        } else {
            diff /= 24;
            return pluralize(diff, "day") + " ago";
        }
    }

    private static String pluralize(long n, String message) {
        return n + " " + message + (n > 1 ? "s" : "");
    }
}
