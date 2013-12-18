package com.psddev.dari.util;
import java.io.IOException;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link VideoStorageItem}
 * Provides methods specific to video storage item. Also provides implementation for listeners
 *
 */
public abstract class VideoStorageItem extends AbstractStorageItem {
    private static final Logger LOGGER = LoggerFactory.getLogger(VideoStorageItem.class);
    public static enum TranscodingStatus {

        PENDING("Pending"),
        SUCCEEDED("Success"),
        FAILED("Failed");

        private String description;

        private TranscodingStatus(String description) {
            this.description = description;
        }

        public String toString() {
            return this.description;
        }

    };

    public static enum DurationType {

        SHORT("Short"),
        MEDIUM("Medium"),
        LONG("Long"),
        NOT_AVAILABLE("Not Available");

        private String description;

        private DurationType(String description) {
            this.description = description;
        }

        public String toString() {
            return this.description;
        }

    };

    private List<UUID> videoStorageItemListenerIds;
    private transient List<VideoStorageItemListener> videoStorageItemListeners;

    public abstract TranscodingStatus getTranscodingStatus();
    public abstract DurationType getDurationType();
    public abstract String getTranscodingError();
    public abstract List<Integer> getTranscodingFlavorIds();
    public abstract String getThumbnailUrl();
    public abstract void setThumbnailUrl(String thumbnailUrl);
    public abstract Long getLength();
    public abstract void delete() throws IOException;
    public abstract boolean pull();
    public abstract void push();
    /*This returns the id of this item in external storage such as Kaltura/Brightcove */
    public abstract String getExternalId();

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
        videoStorageItemListeners = null;
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
