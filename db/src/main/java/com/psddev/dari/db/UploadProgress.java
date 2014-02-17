package com.psddev.dari.db;

import java.util.Date;

/**
 * A record which stores upload progress of a video/photo
 */
public class UploadProgress extends Record {
    @Indexed
    private String key;
    private Long bytesRead;
    private Long contentLength;
    @Indexed
    private Date createdAt;
    private Integer itemIndex;
    private int percentDone;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public long getContentLength() {
        return contentLength;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public int getPercentDone() {
        return percentDone;
    }

    public void setPercentDone(int percentDone) {
        this.percentDone = percentDone;
    }

    public Integer getItemIndex() {
        return itemIndex;
    }

    public void setItemIndex(Integer itemIndex) {
        this.itemIndex = itemIndex;
    }

    public void setBytesRead(Long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public void setContentLength(Long contentLength) {
        this.contentLength = contentLength;
    }

    public void computePercentDone() {
        if (contentLength > 0) {
            percentDone = (int) Math.round(100.00 * bytesRead / contentLength);
        }
    }

    public static class Static {
        public static UploadProgress find(String key) {
            return Query.from(UploadProgress.class).where("key = ?", key).sortDescending("createdAt").first();
        }

        public static void delete(Date date) {
            Query.from(UploadProgress.class).where("createdAt < ?", date).deleteAll();
        }

        public static void delete(String key) {
            Query.from(UploadProgress.class).where("key = ?", key).deleteAll();
        }
    }

}