package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Task to upload a video to Kaltura
 */
public class KalturaUploadTask extends Task {
    private static final String DEFAULT_TASK_NAME = "Kaltura Video Upload Task";
    private final KalturaStorageItem ksi;
    InputStream fileData;
    String entryId;
    String fileName;
    long fileSize;

    public KalturaUploadTask(KalturaStorageItem ksi, InputStream fileData, String entryId, String fileName, long fileSize) {
        super(null, DEFAULT_TASK_NAME);
        this.ksi = ksi;
        this.fileData = fileData;
        this.entryId = entryId;
        this.fileName = fileName;
        this.fileSize = fileSize;
    }

    @Override
    public void doTask() throws IOException {
        try {
            ksi.uploadVideo(fileData, entryId, fileName, fileSize);
        } finally {
            fileData.close();
        }
    }

}
