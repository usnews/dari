package com.psddev.dari.util;
import java.lang.reflect.Method;
import java.util.Date;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.fileupload.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
* Listener which receives notifications about upload progress
*/
public class UploadProgressListener implements ProgressListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadProgressListener.class);
    public static final String UPLOAD_PROGRESS_UNIQUE_KEY_PARAM="dari/uploadProgressUniqueKeyParam";
    public static final String UPLOAD_PROGRESS_INFO_MIN_BYTES_THRESHOLD="dari/uploadProgressInfoMinimumBytesThreshold";
    private static long DEFAULT_UPLOAD_INFO_BYTES_THRESHOLD=250000L;
    /**
     * Upload Progress information  will be updated only when the bytes uploaded
     * exceed the threshold  to reduce the number of update calls
     */
    private long uploadBytesThreshold=DEFAULT_UPLOAD_INFO_BYTES_THRESHOLD;
    private long  prevBytesRead = 0;


    private String uploadProgressKey;
  
    public String getUploadProgressKey() {
        return uploadProgressKey;
    }

    public void setUploadProgressKey(String uploadProgressKey) {
        this.uploadProgressKey = uploadProgressKey;
    }
    

    public long getUploadBytesThreshold() {
        return uploadBytesThreshold;
    }

    public void setUploadBytesThreshold(long uploadBytesThreshold) {
        this.uploadBytesThreshold = uploadBytesThreshold;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void update(long bytesRead, long contentLength, int itemIndex) {
        try {   
         LOGGER.info("uploadBytesThreshold :" + uploadBytesThreshold);
        //Update progres info only if bytes uploaded exceeds uploadBytesThreshold or if the upload is complete and contentLength exceeds threshold
        if (contentLength > -1 && bytesRead-prevBytesRead  >  uploadBytesThreshold ||bytesRead ==contentLength && bytesRead >  uploadBytesThreshold) {   
            prevBytesRead=bytesRead;
            //long t1=System.currentTimeMillis();
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
            LOGGER.debug("Value of upload progress key :" + uploadProgressKey);
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
            //long t2=System.currentTimeMillis();
            //LOGGER.debug("Time taken for updating upload progress :" + (t2 -t1));
        }
        } catch (Exception e) {
            LOGGER.error("update method failed" + e);
        }
    }
    
    public  UploadProgressListener() {
        //Check to see if it's set in the properties..if not use default
        if (  ObjectUtils.to(Long.class, Settings.get(UPLOAD_PROGRESS_INFO_MIN_BYTES_THRESHOLD)) != null ) {
            uploadBytesThreshold=DEFAULT_UPLOAD_INFO_BYTES_THRESHOLD;
        } else {
            uploadBytesThreshold=DEFAULT_UPLOAD_INFO_BYTES_THRESHOLD;
        }
    }
   
    
    public static final class Static {
        public static String getUploadProgressUniqueKey(HttpServletRequest request) {
            Cookie cmsToolUserCookie=JspUtils.getCookie(request, MultipartRequest.USER_COOKIE);
            if (cmsToolUserCookie != null &&  !StringUtils.isEmpty(cmsToolUserCookie.getValue())){
                int indexOfBar=cmsToolUserCookie.getValue().indexOf('|');
                return cmsToolUserCookie.getValue().substring(0,indexOfBar);
            }

            String uploadProgressUniqueKeyParam = ObjectUtils.to(String.class, Settings.get(UPLOAD_PROGRESS_UNIQUE_KEY_PARAM));
            if (! StringUtils.isEmpty(uploadProgressUniqueKeyParam)) {
                String uploadProgressUniqueKey=request.getParameter(uploadProgressUniqueKeyParam); 
                //LOGGER.info("Value of uploadProgressUniqueKey param value..." + uploadProgressUniqueKey);
                if (!StringUtils.isEmpty(uploadProgressUniqueKey)) {
                   return uploadProgressUniqueKey;
                }
            }     
            return null;
        }
  }


}

