package com.psddev.dari.util; 
/**
* UploadInfo abstracts information needed to display upload progress to end user 
*/
public class UploadInfo {
        private long theBytesRead = 0;
        private long theContentLength = -1;
        private int whichItem = 0;
        private int percentDone = 0;
        private boolean contentLengthKnown = false;
        private String message;
        private String fieldName;
        public UploadInfo() {}
        public UploadInfo(UploadProgressListener listener) {
            theBytesRead=listener.getTheBytesRead();
            theContentLength=listener.getTheContentLength();
            whichItem=listener.getWhichItem();
            contentLengthKnown=listener.isContentLengthKnown();
            percentDone=listener.getPercentDone();
            message=listener.getMessage();
        }
        public void setMessage(String message) {
            this.message=message;
        }
        public String getMessage() {
            if (message != null) {
                return message;
            }
            if (theContentLength == -1) {
               return "" + theBytesRead + " of Unknown-Total bytes have been read.";
            } else {
               return "" + theBytesRead + " of " + theContentLength + " bytes have been read (" + percentDone + "% done).";
            }
        }
        public void setFieldName(String fieldName) {
            this.fieldName=fieldName;
        }
        public String getFieldName() {
            return fieldName;
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
