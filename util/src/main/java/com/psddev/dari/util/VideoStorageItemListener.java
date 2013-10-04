package com.psddev.dari.util;
/**
 * Listener interface which will be used to invoke the listening objects when
 * there is a change in video transcoding  status
 */
public interface VideoStorageItemListener {
    public void processTranscodingNotification(VideoStorageItem item);
}
