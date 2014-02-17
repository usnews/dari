package com.psddev.dari.db;

import java.io.IOException;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.RepeatingTask;

/***
 * 
 * This task removes UploadProgress records which are created before 10 mins
 */
public class UploadProgressTask extends RepeatingTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadProgressTask.class);
    private static final String DEFAULT_TASK_NAME = "Upload Progress Task";

    @Override
    protected DateTime calculateRunTime(DateTime currentTime) {
        return every(currentTime, DateTimeFieldType.minuteOfHour(), 0, 10);
    }

    public UploadProgressTask() {
        super(null, DEFAULT_TASK_NAME);
    }

    @Override
    protected void doRepeatingTask(DateTime runTime) throws IOException {
        if (!shouldContinue()) {
            return;
        }
        DistributedLock disLock = null;
        try {
            disLock = DistributedLock.Static.getInstance(Database.Static.getDefault(), UploadProgressTask.class.getName());
            disLock.lock();
            LOGGER.debug("UploadProgressTask starting....");
            UploadProgress.Static.delete(new Date(System.currentTimeMillis() - 10 * 60 * 1000));
            this.setProgress("All Done ....");
        } catch (Exception e) {
            LOGGER.error("UploadProgress task failed ", e);
        } finally {
            if (disLock != null) {
                disLock.unlock();
            }
        }
    }
}
