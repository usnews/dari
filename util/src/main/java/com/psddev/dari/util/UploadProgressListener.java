package com.psddev.dari.util;
import java.util.List;

import org.apache.commons.fileupload.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
* Listener which receives notifications about upload progress
*/
public class UploadProgressListener implements ProgressListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadProgressListener.class);
    private long num500Ks = 0;
    private long theBytesRead = 0;
    private long theContentLength = -1;
    private int whichItem = 0;
    private int percentDone = 0;
    private boolean contentLengthKnown = false;
    private String fieldName;
    private List<String> paramNames;
    public void setParamNames(List<String> paramNames) {
        this.paramNames=paramNames;
    }
    @Override
    public void update(long bytesRead, long contentLength, int itemIndex) {
        try {
        if (contentLength > -1) {
           contentLengthKnown = true;
        }
        theBytesRead = bytesRead;
        theContentLength = contentLength;
        whichItem = itemIndex;
        long nowNum500Ks = bytesRead / 500000;
        LOGGER.info(bytesRead + " of " + theContentLength + " bytes have been read. Index is:" + itemIndex);
        // Only run this code once every 500K..we can change it to  1MB
        if (nowNum500Ks > num500Ks) {
             num500Ks = nowNum500Ks;
             if (contentLengthKnown) {
                 percentDone = (int) Math.round(100.00 * bytesRead / contentLength);
              }
        }
        LOGGER.info("value of percentage done is:" + percentDone);
        } catch (Exception e) {
            LOGGER.error("update method failed" + e);
        }
    }

    
    public String getMessage() {
        if (theContentLength == -1) {
           return new StringBuffer().append(theBytesRead).append(" of Unknown-Total bytes have been read.").toString();
        } else {
           return new StringBuffer().append(theBytesRead).append( " of ").append( theContentLength).append(" bytes have been read (").append( percentDone).append("% done).").toString();
        }
    }
    public long getNum500Ks() {
        return num500Ks;
    }
    public void setNum500Ks(long num500Ks) {
        this.num500Ks = num500Ks;
    }
    public long getTheBytesRead() {
        return theBytesRead;
    }
    public void setTheBytesRead(long theBytesRead) {
        this.theBytesRead = theBytesRead;
    }
    public long getTheContentLength() {
        return theContentLength;
    }
    public void setTheContentLength(long theContentLength) {
        this.theContentLength = theContentLength;
    }
    public int getWhichItem() {
        return whichItem;
    }
    public void setWhichItem(int whichItem) {
        this.whichItem = whichItem;
    }
    public int getPercentDone() {
        return percentDone;
    }
    public void setPercentDone(int percentDone) {
        this.percentDone = percentDone;
    }
    public boolean isContentLengthKnown() {
        return contentLengthKnown;
    }
    public void setContentLengthKnown(boolean contentLengthKnown) {
        this.contentLengthKnown = contentLengthKnown;
    }
}

