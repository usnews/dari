package com.psddev.dari.util;
import org.apache.commons.fileupload.ProgressListener;
import org.apache.commons.fileupload.FileItem;
import java.util.List;
import java.lang.StringBuffer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
* Listener which receives notifications about upload progress
*/
public class UploadProgressListener implements ProgressListener {
    private static final Logger logger = LoggerFactory.getLogger(UploadProgressListener.class);
    private long num100Ks = 0;
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
    public void update(long bytesRead, long contentLength, int itemIndex) {
        if (contentLength > -1) {
           contentLengthKnown = true;
        }
        theBytesRead = bytesRead;
        theContentLength = contentLength;
        whichItem = itemIndex;
        String paramName=paramNames.get(itemIndex);
        fieldName=getFieldNameUsingParamName(paramName);

        long nowNum100Ks = bytesRead / 100000;
        logger.info(bytesRead + " of " + theContentLength + " bytes have been read. Index is:" + itemIndex);
        if (itemIndex < paramNames.size()) {
            logger.info("Param Name using index " + paramNames.get(itemIndex));
        }
        // Only run this code once every 100K
        if (nowNum100Ks > num100Ks) {
             num100Ks = nowNum100Ks;
             if (contentLengthKnown) {
                 percentDone = (int) Math.round(100.00 * bytesRead / contentLength);
              }
        }
    }

    private static String getFieldNameUsingParamName(String paramName) {
        try {
            //Eliminate the object id prefix
            String fieldNameWithType=paramName.substring(paramName.indexOf('/') +1 );
            //Get rid of file suffix
            return fieldNameWithType.substring(0,fieldNameWithType.lastIndexOf('.'));
        } catch (Exception e ) {
            return paramName;
        }
    }
    public String getFieldName() {
        return fieldName;
    }
    public String getMessage() {
        if (theContentLength == -1) {
           return new StringBuffer().append(theBytesRead).append(" of Unknown-Total bytes have been read.").toString();
        } else {
           return new StringBuffer().append(theBytesRead).append( " of ").append( theContentLength).append(" bytes have been read (").append( percentDone).append("% done).").toString();
        }
    }
    public long getNum100Ks() {
        return num100Ks;
    }
    public void setNum100Ks(long num100Ks) {
        this.num100Ks = num100Ks;
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

