package com.psddev.dari.util;
import java.lang.reflect.Method;
import java.util.Date;

import org.apache.commons.fileupload.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
* Listener which receives notifications about upload progress
*/
public class UploadProgressListener implements ProgressListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadProgressListener.class);
    /**
     * Upload Progress information  will be updated only when the bytes uploaded
     * exceed the threshold  to reduce the number of update calls
     */
    private static long uploadBytesThreshold=250000L;
    private long  prevBytesRead = 0;


    private String uploadProgressKey;
  
    public String getUploadProgressKey() {
        return uploadProgressKey;
    }

    public void setUploadProgressKey(String uploadProgressKey) {
        this.uploadProgressKey = uploadProgressKey;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void update(long bytesRead, long contentLength, int itemIndex) {
        try {    
        //Update progres info only if bytes uploaded exceeds uploadBytesThreshold or if the upload is complete
        if (contentLength > -1 && bytesRead-prevBytesRead  >  uploadBytesThreshold ||bytesRead ==contentLength && bytesRead >  uploadBytesThreshold) {   
            prevBytesRead=bytesRead;
            long t1=System.currentTimeMillis();
            
            //First, check to see if an 
            Class uploadProgressClass=Class.forName("com.psddev.dari.db.UploadProgress");
            Class uploadProgressStatic=Class.forName("com.psddev.dari.db.UploadProgress$Static");
            Method method=uploadProgressStatic.getDeclaredMethod("find",String.class);
            Object uploadProgress=method.invoke(uploadProgressStatic,uploadProgressKey);
            //If upload progress object doesn't exist..create a new one 
            if (uploadProgress == null) {
                 uploadProgress=uploadProgressClass.newInstance();
            }
            
            method=uploadProgressClass.getDeclaredMethod("setKey",String.class);
            method.invoke(uploadProgress, uploadProgressKey);
            LOGGER.info("Value of upload progress key :" + uploadProgressKey);
            method=uploadProgressClass.getDeclaredMethod("setBytesRead",Long.class);
            method.invoke(uploadProgress, bytesRead);
            method=uploadProgressClass.getDeclaredMethod("setContentLength",Long.class);
            method.invoke(uploadProgress, contentLength);
            method=uploadProgressClass.getDeclaredMethod("computePercentDone",(Class[] )null);
            method.invoke(uploadProgress, (Object[])null);
            method=uploadProgressClass.getDeclaredMethod("setItemIndex",Integer.class);
            method.invoke(uploadProgress, itemIndex);  
            method=uploadProgressClass.getDeclaredMethod("setCreatedAt",Date.class);
            method.invoke(uploadProgress, new Date());
            //Save the record
            method=uploadProgressClass.getMethod("save", (Class[] )null);
            method.invoke(uploadProgress, (Object[])null);
            long t2=System.currentTimeMillis();
            LOGGER.info("Time taken for this method:" + (t2 -t1));
        }
        } catch (Exception e) {
            LOGGER.error("update method failed" + e);
        }
    }

   

}

