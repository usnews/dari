package com.psddev.dari.util;
import java.util.Map;
import java.lang.String;
import com.kaltura.client.KalturaApiException;
import com.kaltura.client.KalturaClient;
import com.kaltura.client.KalturaConfiguration;
import com.kaltura.client.enums.KalturaSessionType;
import com.kaltura.client.services.KalturaSessionService;

/**
 * Class which provides static utility methods
 * to manage session with Kaltura
 */
public class KalturaSessionUtils {
    public static void startAdminSession(KalturaClient client, KalturaConfiguration kalturaConfig) throws KalturaApiException{
        startSession(client, kalturaConfig, kalturaConfig.getAdminSecret(), KalturaSessionType.ADMIN);
    }
    public static void startSession(KalturaClient client, KalturaConfiguration kalturaConfig, String secret,
            KalturaSessionType type) throws KalturaApiException {
        KalturaSessionService sessionService = client.getSessionService();
        String sessionId = sessionService.start(secret, "admin", type,
                kalturaConfig.getPartnerId(), kalturaConfig.getTimeout(), "");
        client.setSessionId(sessionId);
    }
    
    public static void closeSession(KalturaClient client) throws KalturaApiException {
        client.getSessionService().end();
    }
    public static  KalturaConfiguration getKalturaConfig() {
         KalturaConfiguration kalturaConfig = new KalturaConfiguration();
         Map<String,Object> settings=(Map<String,Object>) Settings.get(KalturaStorageItem.KALTURA_SETTINGS_PREFIX);
         kalturaConfig.setPartnerId(ObjectUtils.to(Integer.class,settings.get(KalturaStorageItem.KALTURA_PARTNER_ID_SETTING)));
         kalturaConfig.setSecret(ObjectUtils.to(String.class,settings.get(KalturaStorageItem.KALTURA_SECRET_SETTING)));
         kalturaConfig.setAdminSecret(ObjectUtils.to(String.class,settings.get(KalturaStorageItem.KALTURA_ADMIN_SECRET_SETTING)));
         kalturaConfig.setEndpoint(ObjectUtils.to(String.class,settings.get(KalturaStorageItem.KALTURA_END_POINT_SETTING)));
         return kalturaConfig;
    }
}
