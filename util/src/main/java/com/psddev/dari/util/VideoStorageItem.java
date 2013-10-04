package com.psddev.dari.util; 
import java.io.IOException;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link VideoStorageItem} 
 * Provides methods specific to video storage item. Also provides implementation for  listeners
 * 
 */
public abstract class  VideoStorageItem extends AbstractStorageItem {
    private static final Logger LOGGER = LoggerFactory.getLogger(VideoStorageItem.class);
    public enum TranscodingStatus { PENDING,SUCCEEDED,FAILED}
    public abstract TranscodingStatus getTranscodingStatus();
    public abstract String getTranscodingError();
    public abstract  String getThumbnailUrl();
    public abstract Long getLength();
    public abstract void delete() throws IOException;
    public abstract boolean pull();
    public abstract void push();
    
    private List<UUID> videoStorageItemListenerIds;
    private transient List<VideoStorageItemListener> videoStorageItemListeners;
    public List<UUID> getVideoStorageItemListenerIds() {
        return videoStorageItemListenerIds;
    }
    public void setVideoStorageItemListeners( List<VideoStorageItemListener> videoStorageItemListeners) {
        this.videoStorageItemListeners = videoStorageItemListeners;
    }
    /** UUID of a record which implements VideoStorageItemListener interface **/
    public void registerVideoStorageItemListener(UUID listenerId) {
        //LOGGER.info("Value of listener in registerVideoStorageItemListener is:" + listener);
        if (videoStorageItemListenerIds == null) {
             resetVideoStorageItemListeners();
        }
        videoStorageItemListenerIds.add(listenerId);
    }
    public void resetVideoStorageItemListeners() {
        videoStorageItemListenerIds = new ArrayList<UUID>();
        videoStorageItemListeners =null;
    }
    
    public void notifyVideoStorageItemListeners() {
        for (VideoStorageItemListener listener : videoStorageItemListeners) {
            try {
                listener.processTranscodingNotification(this);
            } catch (Exception error) {
                LOGGER.error(String.format("Can't execute [%s] on [%s]!",
                        listener, this), error);
            }
        }
    }
}
