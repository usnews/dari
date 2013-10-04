package com.psddev.dari.util; 
import java.io.IOException;
import java.util.UUID;
import java.util.List;

/**
 * {@link VideoStorageItem} 
 * Provides methods to access most commonly used information of a video stored
 * using KalturaStorageItem or BrightCoveStorageItem
 */
public interface VideoStorageItem extends StorageItem {
    public enum TranscodingStatus { PENDING,SUCCEEDED,FAILED}
    public TranscodingStatus getTranscodingStatus();
    public String getTranscodingError();
    String getThumbnailUrl();
    Long getLength();
    void delete() throws IOException;
    public boolean pull();
    public void push();
    public void registerVideoStorageItemListener(UUID listenerId);
    public List<UUID> getVideoStorageItemListenerIds();
    public void resetVideoStorageItemListeners();
    public void setVideoStorageItemListeners( List<VideoStorageItemListener> videoStorageItemListeners);
    public void notifyVideoStorageItemListeners();
}
