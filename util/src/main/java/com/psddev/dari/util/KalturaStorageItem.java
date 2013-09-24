package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.client.KalturaApiException;
import com.kaltura.client.KalturaClient;
import com.kaltura.client.KalturaConfiguration;
import com.kaltura.client.enums.KalturaEntryType;
import com.kaltura.client.enums.KalturaMediaType;
import com.kaltura.client.enums.KalturaSessionType;
import com.kaltura.client.services.KalturaSessionService;
import com.kaltura.client.types.KalturaMediaEntry;
import com.kaltura.client.types.KalturaUploadToken;
import com.kaltura.client.types.KalturaUploadedFileTokenResource;

/**
 * {@link StorageItem} stored in
 * <a href="http://www.kaltura.com/">Kaltura</a>.
 *
 * <p>To use this class, please add log4j dependency 
 * <a href="http://logging.apache.org/log4j">Log4j Logging library</a>.
 * If you use Maven, you should add the following dependency:</p>
 *
 * <blockquote><pre><code data-type="xml">{@literal
 *<dependency>
 *    <groupId>log4j</groupId>
 *    <artifactId>log4j</artifactId>
 *    <version>1.2.16</version>
 *</dependency>}</code></pre></blockquote>
 */

public class KalturaStorageItem extends AbstractStorageItem {
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

   
    private transient String secret;
    private transient String adminSecret;
    private transient String endPoint;
    private transient Integer partnerId;
    private transient Integer conversionProfileId;
    private transient Integer sessionTimeout;
    private transient Integer playerId;
    private transient String  playerKey;

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

    /** Object fields on Kaltura Video object. More can be added later. **/
    private String kalturaId;
    private String name;
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    
    /** Session management related method using Kaltura Client Library*/
    private static void startAdminSession(KalturaClient client, KalturaConfiguration kalturaConfig) throws KalturaApiException{
        startSession(client, kalturaConfig, kalturaConfig.getAdminSecret(), KalturaSessionType.ADMIN);
    }
    
    private static void startSession(KalturaClient client, KalturaConfiguration kalturaConfig, String secret,
            KalturaSessionType type) throws KalturaApiException {
        
        KalturaSessionService sessionService = client.getSessionService();

        String sessionId = sessionService.start(secret, "admin", type,
                kalturaConfig.getPartnerId(), kalturaConfig.getTimeout(), "");
        client.setSessionId(sessionId);
    }
    
    private static void closeSession(KalturaClient client) throws KalturaApiException {
        client.getSessionService().end();
    }

    private String uploadVideo(InputStream fileData, String fileName, long fileSize)  {
        try {
            KalturaConfiguration kalturaConfig = new KalturaConfiguration();
            kalturaConfig.setPartnerId(getPartnerId());
            kalturaConfig.setSecret(getSecret());
            kalturaConfig.setAdminSecret(getAdminSecret());
            kalturaConfig.setEndpoint(getEndPoint());
            KalturaClient client= new KalturaClient(kalturaConfig);
            startAdminSession(client, kalturaConfig);
            // Create entry
            KalturaMediaEntry entry = new KalturaMediaEntry();
            // Can be enhanced latter to populate the name from the video object
            setName(fileName);
            entry.name =  getName();
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
            closeSession(client);
            //assertNotNull(uploadToken);
            LOGGER.info("Value of entry id is:" +entry.id );
            
            //After the upload is successful..set kaltura id and path to this URL.
            //We can add more attributes from Kaltura if needed
            kalturaId = entry.id;
            setPath(entry.dataUrl);
            return kalturaId;
                
        } catch (Exception e) {
            LOGGER.error("Video Upload Failed to Kaltura :" + e.getMessage(),e);
            return "";
        }
    }
    
    @Override
    /***
     * Uses originalFilename and content length from metadata
     */
    public void saveData(InputStream data) throws IOException {
        try {
            LOGGER.info("Control in saveData method");
            String originalFileName = (String) getMetadata().get(METADATA_PARAM_ORIGINAL_FILE_NAME);
            if(StringUtils.isBlank(originalFileName)) {
                throw new IllegalArgumentException( METADATA_PARAM_ORIGINAL_FILE_NAME + "not set in metadata.");
            }
            Map<String,Object> headersMap = ObjectUtils.to(new TypeReference<Map<String,Object>>() {}, getMetadata().get(HTTP_HEADERS));
            if(headersMap == null) {
                throw new IllegalArgumentException( HTTP_HEADERS + " not set in metadata.");
            }
            List<String> contentLengthList = ObjectUtils.to(new TypeReference<List<String>>() {}, headersMap.get(HTTP_HEADER_CONTENT_LENGTH));
            if(contentLengthList == null) {
                throw new IllegalArgumentException( HTTP_HEADER_CONTENT_LENGTH + " not set in " + HTTP_HEADERS + " metadata.");
            }
            long fileSizeBytes = Long.parseLong(contentLengthList.get(0));
            kalturaId = uploadVideo(data, originalFileName, fileSizeBytes);
            LOGGER.info("Value of fileName is:" + originalFileName);
            LOGGER.info("Value of fileSize Bytes is:" + fileSizeBytes);
        } catch (Exception e) {
            LOGGER.error("KalturaStorageItem.saveData failed", e);
        }
    }    

    @Override
    protected InputStream createData() throws IOException {
        LOGGER.info("Control in createData method");
        return new URL(getPublicUrl()).openStream();
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

}
