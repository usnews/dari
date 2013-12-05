package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.client.KalturaApiException;
import com.kaltura.client.KalturaClient;
import com.kaltura.client.KalturaConfiguration;
import com.kaltura.client.enums.KalturaEntryStatus;
import com.kaltura.client.enums.KalturaEntryType;
import com.kaltura.client.enums.KalturaMediaType;
import com.kaltura.client.types.KalturaMediaEntry;
import com.kaltura.client.types.KalturaUploadToken;
import com.kaltura.client.types.KalturaUploadedFileTokenResource;

/**
 * {@link StorageItem} stored in
 * <a href="http://www.kaltura.com/">Kaltura</a>.
 */

public class KalturaStorageItem extends VideoStorageItem {
    private static final Logger LOGGER = LoggerFactory.getLogger(KalturaStorageItem.class);

    /** Setting key for Kaltura's API secret */
    public static final String KALTURA_SECRET_SETTING = "secret";
    /** Setting key for Kaltura's API admin secret */
    public static final String KALTURA_ADMIN_SECRET_SETTING = "adminSecret";
     /** Setting key for Kaltura's API endpoint */
    public static final String KALTURA_END_POINT_SETTING = "endPoint";
     /** Setting key for Kaltura partner id */
    public static final String KALTURA_PARTNER_ID_SETTING="partnerId";
     /** Setting key for Kaltura conversion/transcoding  profile id */
    public static final String KALTURA_CONVERSION_PROFILE_ID_SETTING="conversionProfileId";
     /** Setting key for Kaltura Session Timeout setting */
    public static final String KALTURA_SESSION_TIMEOUT_SETTING="sessionTimeout";
     /** Setting key for Kaltura Player Id. Internally referred as UiConfId in Kaltura API*/
    public static final String KALTURA_PLAYER_ID_SETTING="playerId";
     /** Setting key for Kaltura player key. Please look at the embed code to identify these values */
    public static final String KALTURA_PLAYER_KEY_SETTING = "playerKey";
     /** Kaltura settings prefix  */
    public static final String KALTURA_SETTINGS_PREFIX = "dari/storage/kaltura";

    private transient String secret;
    private transient String adminSecret;
    private transient String endPoint;
    private transient Integer partnerId;
    private transient Integer conversionProfileId;
    private transient Integer sessionTimeout;
    private transient Integer playerId;
    private transient String playerKey;

    /** Object fields on Kaltura Video object. More can be added later. **/
    private String kalturaId;
    private String name;
    private KalturaEntryStatus status;
    private Long length;
    private transient KalturaMediaEntry mediaEntry;

    public String getPlayerKey() {
        return playerKey;
    }

    public void setPlayerKey(String playerKey) {
        this.playerKey = playerKey;
    }

    public Integer getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Integer sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getAdminSecret() {
        return adminSecret;
    }

    public void setAdminSecret(String adminSecret) {
        this.adminSecret = adminSecret;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public Integer getPartnerId() {
        return partnerId;
    }

    public void setPartnerId(Integer partnerId) {
        this.partnerId = partnerId;
    }

    public Integer getConversionProfileId() {
        return conversionProfileId;
    }

    public void setConversionProfileId(Integer conversionProfileId) {
        this.conversionProfileId = conversionProfileId;
    }

    public Integer getPlayerId() {
        return playerId;
    }

    public void setPlayerId(Integer playerId) {
        this.playerId = playerId;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        super.initialize(settingsKey, settings);

        setSecret(ObjectUtils.to(String.class, settings.get(KALTURA_SECRET_SETTING)));
        if(StringUtils.isBlank(getSecret())) {
            throw new SettingsException(settingsKey + "/" + KALTURA_SECRET_SETTING, "No kaltura secret.");
        }

        setAdminSecret(ObjectUtils.to(String.class, settings.get(KALTURA_ADMIN_SECRET_SETTING)));
        if(StringUtils.isBlank(getAdminSecret())) {
            throw new SettingsException(settingsKey + "/" + KALTURA_ADMIN_SECRET_SETTING, "No kaltura secret.");
        }

        setEndPoint(ObjectUtils.to(String.class, settings.get(KALTURA_END_POINT_SETTING)));
        if(StringUtils.isBlank(getEndPoint())) {
            throw new SettingsException(settingsKey + "/" + KALTURA_END_POINT_SETTING, "No kaltura endpoint.");
        }

        setPartnerId(ObjectUtils.to(Integer.class, settings.get(KALTURA_PARTNER_ID_SETTING)));
        if(ObjectUtils.isBlank(getPartnerId())) {
            throw new SettingsException(settingsKey + "/" + KALTURA_PARTNER_ID_SETTING, "No kaltura partnerId.");
        }

        setConversionProfileId(ObjectUtils.to(Integer.class, settings.get(KALTURA_CONVERSION_PROFILE_ID_SETTING)));
        if(ObjectUtils.isBlank(getConversionProfileId())) {
            throw new SettingsException(settingsKey + "/" + KALTURA_CONVERSION_PROFILE_ID_SETTING, "No kaltura conversionProfileId.");
        }

        setSessionTimeout(ObjectUtils.to(Integer.class, settings.get(KALTURA_SESSION_TIMEOUT_SETTING)));
        if(ObjectUtils.isBlank(getSessionTimeout())) {
            throw new SettingsException(settingsKey + "/" + KALTURA_SESSION_TIMEOUT_SETTING, "No kaltura session timeout .");
        }

        setPlayerId(ObjectUtils.to(Integer.class, settings.get(KALTURA_PLAYER_ID_SETTING)));
        if(ObjectUtils.isBlank(getPlayerId())) {
            throw new SettingsException(settingsKey + "/" + KALTURA_PLAYER_ID_SETTING, "No kaltura player id.");
        }

        setPlayerKey(ObjectUtils.to(String.class, settings.get(KALTURA_PLAYER_KEY_SETTING)));
        if(ObjectUtils.isBlank(getPlayerKey())) {
            throw new SettingsException(settingsKey + "/" + KALTURA_PLAYER_ID_SETTING, "No kaltura player key.");
        }

    }

    public String getKalturaId() {
        return kalturaId;
    }

    public void setKalturaId(String kalturaId) {
        this.kalturaId = kalturaId;
    }

    public String getExternalId() {
        return getKalturaId();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public KalturaEntryStatus getStatus() {
        return status;
    }

    public void setStatus(KalturaEntryStatus status) {
        this.status = status;
    }

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public KalturaMediaEntry getMediaEntry() {
        return mediaEntry;
    }

    public void setMediaEntry(KalturaMediaEntry mediaEntry) {
        this.mediaEntry = mediaEntry;
    }

    private String uploadVideo(InputStream fileData, String fileName, long fileSize)  {
        try {
            KalturaConfiguration kalturaConfig = KalturaSessionUtils.getKalturaConfig();
            KalturaClient client= new KalturaClient(kalturaConfig);
            KalturaSessionUtils.startAdminSession(client, kalturaConfig);
            // Create entry
            KalturaMediaEntry entry = new KalturaMediaEntry();
            //Can be enhanced latter to populate the name  from the video object
            setName(fileName);
            entry.name = getName();
            entry.type = KalturaEntryType.MEDIA_CLIP;
            entry.mediaType = KalturaMediaType.VIDEO;
            entry.conversionProfileId=getConversionProfileId();
            entry = client.getMediaService().add(entry);
            //assertNotNull(entry);

            //testIds.add(entry.id);

            // Create token
            KalturaUploadToken uploadToken = new KalturaUploadToken();
            uploadToken.fileName = fileName;
            //uploadToken.fileName = KalturaTestConfig.UPLOAD_VIDEO;
            uploadToken.fileSize = fileSize;
            KalturaUploadToken token = client.getUploadTokenService().add(uploadToken);
            //assertNotNull(token);

            // Define content
            KalturaUploadedFileTokenResource resource = new KalturaUploadedFileTokenResource();
            resource.token = token.id;
            entry = client.getMediaService().addContent(entry.id, resource);
            //assertNotNull(entry);

            // upload
            uploadToken = client.getUploadTokenService().upload(token.id, fileData, fileName, fileSize, false);

            //Once done..close the session
            KalturaSessionUtils.closeSession(client);
            //assertNotNull(uploadToken);
            LOGGER.debug("Value of entry id is:" +entry.id );

            //After the upload is successful..set kaltura id and path to this URL.
            //We can add more attributes from Kaltura if needed
            kalturaId = entry.id;
            setPath(entry.dataUrl);
            return kalturaId;

        } catch (Exception e) {
            LOGGER.error("Video  Upload Failed to Kaltura :" + e.getMessage(),e);
            return "";
        }
    }

    /**
     * Uses originalFilename and content length from metadata
     */
    @Override
    public void saveData(InputStream data) throws IOException {
        try {
            LOGGER.debug("Control in saveData method");
            String originalFileName=(String)getMetadata().get(METADATA_PARAM_ORIGINAL_FILE_NAME);
            if (StringUtils.isBlank(originalFileName)) {
                throw new IllegalArgumentException( METADATA_PARAM_ORIGINAL_FILE_NAME + " not set in metadata.");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> headersMap = (Map<String, Object>)getMetadata().get(HTTP_HEADERS);
            if (headersMap == null) {
                throw new IllegalArgumentException(HTTP_HEADERS + " not set in metadata.");
            }
            @SuppressWarnings("unchecked")
            List<String> contentLengthList = (List<String>)headersMap.get(HTTP_HEADER_CONTENT_LENGTH);
            if (contentLengthList == null) {
                throw new IllegalArgumentException(HTTP_HEADER_CONTENT_LENGTH + " not set in " + HTTP_HEADERS + " metadata.");
            }
            long fileSizeBytes = Long.parseLong(contentLengthList.get(0));
            kalturaId = uploadVideo(data, originalFileName, fileSizeBytes);
            status = KalturaEntryStatus.PENDING;
            LOGGER.debug("Value of fileName is:" + originalFileName);
            LOGGER.debug("Value of fileSize Bytes is:" + fileSizeBytes);
        } catch (Exception e) {
            LOGGER.error("KalturaStorageItem.saveData failed", e);
        }
    }

    @Override
    protected InputStream createData() throws IOException {
        LOGGER.debug("Control in createData method");
        return new URL(getPublicUrl()).openStream();
    }

    /**
     * Deletes entry
     * @throws IOException
     */
    public void delete() throws IOException {
        LOGGER.debug("Control in delete method");
        try {
            KalturaConfiguration kalturaConfig = KalturaSessionUtils.getKalturaConfig();
            KalturaClient client = new KalturaClient(kalturaConfig);
            KalturaSessionUtils.startAdminSession(client, kalturaConfig);
            // Step2: Delete Media Entry from kaltura
            client.getMediaService().delete(kalturaId);
            // Once done..close the session
            KalturaSessionUtils.closeSession(client);
        } catch (KalturaApiException e) {
            LOGGER.error("Failed to delete kaltura entry id", e);
        }
    }

    /***
     * Public Url to access video on kaltura
     */
    @Override
    public String getPublicUrl() {
        return getPath();
    }

    @Override
    public boolean isInStorage() {
        if (StringUtils.isBlank(getPath())) return false;
        return true;
    }

    /**
     * Returns default thumbnail
     * @return
     */
    public String getThumbnailUrl() {
        return new StringBuffer(getBaseUrl()).append(getPartnerId().toString())
                .append("/thumbnail/entry_id/")
                .append(getKalturaId()).toString();
    }


    /**
     * Returns thumbnail with a specified width and height at a specified
     * time
     * @param width
     * @param height
     * @param seconds
     * @return
     */
    public String getThumbnailUrl(Integer width,Integer height,Integer seconds) {
        return new StringBuffer(getBaseUrl()).append(getPartnerId().toString()).
                    append("/thumbnail/entry_id/").append(getKalturaId()).
                    append("/width/").append(width).append("/height/").append(height).
                    append("/vid_sec/").append(seconds).toString();
    }

    /**
     * Pulls information from kaltura's storage
     */
    public boolean pull() {
        // Step1: Start kaltura sesion
        KalturaConfiguration kalturaConfig = KalturaSessionUtils.getKalturaConfig();
        KalturaClient client = new KalturaClient(kalturaConfig);
        try {
            KalturaSessionUtils.startAdminSession(client, kalturaConfig);
            // Step2: Get the Media Entry from kaltura
            if (kalturaId == null || "".equals(kalturaId)) {
                LOGGER.error("Kaltura ID is empty for video: " + this);
                return false;
            }
            mediaEntry = client.getMediaService().get(kalturaId);
            if (mediaEntry != null) {
                length = new Long(mediaEntry.duration);
                //If there is a change in transcodingStatus, update listeners if added
                //if (status != mediaEntry.status && videoStorageItemListeners != null) {
                 //     notifyVideoStorageItemListeners();
                //}
                status = mediaEntry.status;
            }
            // Once done..close the session
            KalturaSessionUtils.closeSession(client);
        } catch (KalturaApiException e) {
            LOGGER.error("Failed to pull kaltura entry id ", e);
            return false;
        }
        return true;
    }

    /**
     * Push any updates such as tags,adminTags,categories etc to kaltura storage item
     * This method don't update video information.
     */
    public void push() {
        // Step1: Start kaltura sesion
        KalturaConfiguration kalturaConfig = KalturaSessionUtils.getKalturaConfig();
        KalturaClient client = new KalturaClient(kalturaConfig);
        try {
            KalturaSessionUtils.startAdminSession(client, kalturaConfig);
            // Step2: Get the Media Entry from kaltura
            client.getMediaService().update(kalturaId, mediaEntry);
            // Once done..close the session
            KalturaSessionUtils.closeSession(client);
        } catch (KalturaApiException e) {
            LOGGER.error("Failed to update kaltura entry id", e);
        }
    }

    public DurationType getDurationType() {
        Long videoLength=getLength();
        if (videoLength == null ) return DurationType.NOT_AVAILABLE;
        //Upto 4 minutes..short
        if (videoLength.longValue() <= 240) {
             return DurationType.SHORT;
        }
        //4 to 10 minutes...medium. Greater than 10 is LONG
        if (videoLength.longValue() > 240 && videoLength.longValue() <= 600 ) {
             return DurationType.MEDIUM;
        }
        return DurationType.LONG;
    }

    public List<Integer> getTranscodingFlavorIds() {
        if (mediaEntry == null || mediaEntry.flavorParamsIds == null) {
            return null;
        }
        List<String> flavorParamIdStringList = Arrays.asList(mediaEntry.flavorParamsIds.split(","));
        List<Integer> flavorIdList = new ArrayList<Integer>(flavorParamIdStringList.size());
        for (String flavorId : flavorParamIdStringList) {
            flavorIdList.add(new Integer(flavorId));
        }
        return flavorIdList;
    }

    public TranscodingStatus getTranscodingStatus() {
        if (status.equals(KalturaEntryStatus.READY)) {
            return TranscodingStatus.SUCCEEDED;
        } else if (status.equals(KalturaEntryStatus.PENDING) ||
                status.equals(KalturaEntryStatus.PRECONVERT)) {
            return TranscodingStatus.PENDING;
        } else {
            return TranscodingStatus.FAILED;
        }
    }

    public String getTranscodingError() {
        switch (status) {
            case ERROR_CONVERTING:
                return "Error Converting";
            case SCAN_FAILURE:
                return "Virus Scan Failure";
            case INFECTED:
                return "Virus Infected";
            case NO_CONTENT:
                return "No Content";
            case BLOCKED:
                return "Blocked";
            case MODERATE:
                return "Pending Moderation";
            default:
                return "";
        }
    }
}
