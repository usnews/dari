package com.psddev.dari.util;

import static com.psddev.dari.util.AbstractStorageItem.BASE_URL_SUB_SETTING;
import static com.psddev.dari.util.AbstractStorageItem.HTTP_HEADERS;
import static com.psddev.dari.util.AbstractStorageItem.HTTP_HEADER_CONTENT_LENGTH;
import static com.psddev.dari.util.AbstractStorageItem.METADATA_PARAM_ORIGINAL_FILE_NAME;
import static com.psddev.dari.util.AbstractStorageItem.SECURE_BASE_URL_SUB_SETTING;
import static com.psddev.dari.util.KalturaStorageItem.KALTURA_ADMIN_SECRET_SETTING;
import static com.psddev.dari.util.KalturaStorageItem.KALTURA_CONVERSION_PROFILE_ID_SETTING;
import static com.psddev.dari.util.KalturaStorageItem.KALTURA_END_POINT_SETTING;
import static com.psddev.dari.util.KalturaStorageItem.KALTURA_PARTNER_ID_SETTING;
import static com.psddev.dari.util.KalturaStorageItem.KALTURA_PLAYER_ID_SETTING;
import static com.psddev.dari.util.KalturaStorageItem.KALTURA_PLAYER_KEY_SETTING;
import static com.psddev.dari.util.KalturaStorageItem.KALTURA_SECRET_SETTING;
import static com.psddev.dari.util.KalturaStorageItem.KALTURA_SESSION_TIMEOUT_SETTING;
import static org.junit.Assert.assertNotNull;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/***
 * Class to test Kaltura Storage Item.
 * Update settings using your configuration information and remove the @Ignore annotation to execute this test 
 * Also add log4j dependency. Updated your POM to add log4j dependency if your are using maven.
 * @author rmadupuru
 */
@Ignore
public class KalturaStorageItemTest {
	  private static final Logger log = LoggerFactory.getLogger(KalturaStorageItemTest.class);
	  KalturaStorageItem kalturaStorageItem;
	  @Test
	  public void testVideoUpload() throws Exception {
		try {
		Map<String, Object> settings= new HashMap<String, Object>();
		//Update using using your own settings before running this test
		settings.put(KALTURA_SECRET_SETTING, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		settings.put(KALTURA_ADMIN_SECRET_SETTING, "xxxxxxxxxxxxxxxxxxxxxxxx");
		settings.put(KALTURA_END_POINT_SETTING, "http://www.kaltura.com");
		settings.put(KALTURA_PARTNER_ID_SETTING, "xxxxxx");
		settings.put(KALTURA_CONVERSION_PROFILE_ID_SETTING, "3474302");
		settings.put(KALTURA_SESSION_TIMEOUT_SETTING, "300");
		//Please look at the embed code to identify  these values.
		settings.put(KALTURA_PLAYER_ID_SETTING, 12311262);
		settings.put(KALTURA_PLAYER_KEY_SETTING, "1363445590");
	
		kalturaStorageItem= new KalturaStorageItem();
		kalturaStorageItem.initialize("kaltura", settings);
		  
		InputStream input = new URL("http://media3.kicksuite.com/videos/1823608.mp4").openStream();
		Map<String, Object> metadata= new HashMap<String,Object>();
   	    	metadata.put(METADATA_PARAM_ORIGINAL_FILE_NAME,"1823608.mp4");
         	Map<String, Object> headersMap= new HashMap<String, Object>();
   	        List<String> headersValueList= new ArrayList<String>(1);
   	        headersValueList.add("5063060");
   	        headersMap.put(HTTP_HEADER_CONTENT_LENGTH, headersValueList);
   	        metadata.put(HTTP_HEADERS, headersMap);
   	        kalturaStorageItem.setMetadata(metadata);
   	        kalturaStorageItem.saveData(input);
   	        log.info("Value of storage path is:"+ kalturaStorageItem.getPath());
   	        assertNotNull(kalturaStorageItem.getKalturaId());
   	        kalturaStorageItem.getPublicUrl();
		} catch( Exception e) {
			  log.error("testVideoUpload failed on KalturaStorageItem.." + e.getMessage(),e);
		}
	  }
	 
	
}
