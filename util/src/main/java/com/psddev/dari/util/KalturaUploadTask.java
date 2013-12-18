package com.psddev.dari.util; 
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.client.enums.KalturaEntryStatus;
import com.psddev.dari.util.KalturaStorageItem;
import com.psddev.dari.util.StorageItem;
/**
 * Task to upload a video to Kaltura 
*/
public  class KalturaUploadTask extends Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(KalturaUploadTask.class);
    private static final String DEFAULT_TASK_NAME = "Kaltura Video Upload Task";
    private KalturaStorageItem ksi;
    InputStream fileData;
    String entryId;
    String fileName;
    long fileSize;
    public KalturaUploadTask(KalturaStorageItem ksi,InputStream fileData,String entryId,String fileName,long fileSize) {
        super(null, DEFAULT_TASK_NAME);
        this.ksi=ksi;
        this.fileData=fileData;
        this.entryId=entryId;
        this.fileName=fileName;
        this.fileSize=fileSize;
    }
    public void doTask() throws IOException {
        ksi.uploadVideo(fileData, entryId, fileName, fileSize);
    }

}
