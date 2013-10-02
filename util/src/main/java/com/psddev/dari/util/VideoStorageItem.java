package com.psddev.dari.util;

import java.io.IOException;

/**
 * {@link VideoStorageItem} 
 * Provides methods to access most commonly used information of a video stored
 * using KalturaStorageItem or BrightCoveStorageItem
 */
public interface VideoStorageItem extends StorageItem {
    String getThumbnailUrl();
    Long getLength();
    void delete() throws IOException;
    public void resetVideoStorageItemListeners();
    public void registerVideoStorageItemListener(VideoStorageItemListener listener);
}
