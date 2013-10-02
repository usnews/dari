package com.psddev.dari.util;

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
}
