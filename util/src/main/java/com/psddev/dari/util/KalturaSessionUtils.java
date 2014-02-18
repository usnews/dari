package com.psddev.dari.util;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(KalturaSessionUtils.class);
    private final static long CACHE_EXPIRY = 60 * 60 * 1000; // Refresh every hr
    private static void startAdminSession(KalturaClient client, KalturaConfiguration kalturaConfig) throws KalturaApiException {
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
    
    private static KalturaClient getKalturaSession() {
        try {
            KalturaConfiguration kalturaConfig=getKalturaConfig();
            KalturaClient client= new KalturaClient(kalturaConfig);
            startSession(client, kalturaConfig, kalturaConfig.getAdminSecret(), KalturaSessionType.ADMIN);
            return client;
        } catch (Exception e) {
            logger.error("Failed to get kaltura session Id:" , e );
            return null;
        }
    }

    private static KalturaConfiguration getKalturaConfig() {
         KalturaConfiguration kalturaConfig = new KalturaConfiguration();
         @SuppressWarnings("unchecked")
         Map<String,Object> settings=(Map<String,Object>) Settings.get(KalturaStorageItem.KALTURA_SETTINGS_PREFIX);
         kalturaConfig.setPartnerId(ObjectUtils.to(Integer.class,settings.get(KalturaStorageItem.KALTURA_PARTNER_ID_SETTING)));
         kalturaConfig.setSecret(ObjectUtils.to(String.class,settings.get(KalturaStorageItem.KALTURA_SECRET_SETTING)));
         kalturaConfig.setAdminSecret(ObjectUtils.to(String.class,settings.get(KalturaStorageItem.KALTURA_ADMIN_SECRET_SETTING)));
         kalturaConfig.setEndpoint(ObjectUtils.to(String.class,settings.get(KalturaStorageItem.KALTURA_END_POINT_SETTING)));
         kalturaConfig.setTimeout(ObjectUtils.to(Integer.class,settings.get(KalturaStorageItem.KALTURA_SESSION_TIMEOUT_SETTING)));
         return kalturaConfig;
    }
    
    private static PullThroughValue<KalturaClient> cache = new PullThroughValue<KalturaClient>() {

        @Override
        protected KalturaClient produce() {
            //If old session exists..close it
            try {
            KalturaClient oldSession=get();
            if (oldSession != null ) {
                KalturaSessionUtils.closeSession(oldSession);
            }
            } catch (Exception e) {
                logger.debug("Failed to close old kaltura session",e);
            }
            //create a brand new session
            return KalturaSessionUtils.getKalturaSession();
        }

        @Override
        protected boolean isExpired(Date lastProduceDate) {
            // Check if the cache has expired, return true if it has, otherwise
            // false
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastProduceDate.getTime() > CACHE_EXPIRY) {
                logger.debug("Cache expired -  refreshing cache");
                return true;
            }
            return false;
        }
    };
    
    /**
     * Returns kaltura session value from the cache
     */
    public static KalturaClient getSession() {
        return cache.get();
    }
    public static String getSessionId() {
        return cache.get().getSessionId();
    }
    
    public static void closeSession() {
        try {
        closeSession(cache.get());
        } catch(Exception e) {
            logger.debug("Failed to close kaltura session ");
        }
    }

}
