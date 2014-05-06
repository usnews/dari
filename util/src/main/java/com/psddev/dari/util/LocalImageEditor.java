package com.psddev.dari.util;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import javax.imageio.ImageIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalImageEditor extends AbstractImageEditor {

    private static final String DEFAULT_IMAGE_FORMAT = "png";
    private static final String DEFAULT_IMAGE_CONTENT_TYPE = "image/" + DEFAULT_IMAGE_FORMAT;

    protected static final Logger LOGGER = LoggerFactory.getLogger(LocalImageEditor.class);

    @Override
    public StorageItem edit(StorageItem storageItem, String command, Map<String, Object> options, Object... arguments) {
        BufferedImage bufferedImage = null;
        ByteArrayOutputStream ouputStream = new ByteArrayOutputStream();

        try {

            if (storageItem.getPublicUrl().endsWith("tif") || storageItem.getPublicUrl().endsWith("tiff")) {
                //TODO add support for tif
            } else {
                bufferedImage = ImageIO.read(new URL(storageItem.getPublicUrl()));
            }

            if (bufferedImage == null) {
                 LOGGER.error("can't read image " + storageItem.getPublicUrl());
            }

            if (bufferedImage != null) {
                Integer width = null;
                Integer height = null;

                StringBuilder path = new StringBuilder();
                path.append(LocalImageServlet.LEGACY_PATH)
                    .append(command)
                    .append("/");

                if (ImageEditor.CROP_COMMAND.equals(command)) {
                    Integer x = ObjectUtils.to(Integer.class, arguments[0]);
                    Integer y = ObjectUtils.to(Integer.class, arguments[1]);
                    width = ObjectUtils.to(Integer.class, arguments[2]);
                    height = ObjectUtils.to(Integer.class, arguments[3]);
                    bufferedImage = LocalImage.Crop(bufferedImage, x, y, width, height);

                    path.append(x)
                        .append("x")
                        .append(y)
                        .append("x")
                        .append(width)
                        .append(height);

                } else if (ImageEditor.RESIZE_COMMAND.equals(command)) {
                    width = ObjectUtils.to(Integer.class, arguments[0]);
                    height = ObjectUtils.to(Integer.class, arguments[1]);
                    bufferedImage = LocalImage.Resize(bufferedImage, width, height);

                    if (width != null) {
                        path.append(width);
                    }
                    path.append("x");
                    if (height != null) {
                        path.append(height);
                    }

                }
                path.append("?url=").append(storageItem.getPublicUrl());

                UrlStorageItem newStorageItem = StorageItem.Static.createUrl("");

                String format = DEFAULT_IMAGE_FORMAT;
                String contentType = DEFAULT_IMAGE_CONTENT_TYPE;
                if (storageItem.getContentType() != null && storageItem.getContentType().contains("/")) {
                    contentType = storageItem.getContentType();
                    format = storageItem.getContentType().split("/")[1];
                }
                ImageIO.write(bufferedImage, format, ouputStream);
                newStorageItem.setData(new ByteArrayInputStream(ouputStream.toByteArray()));
                newStorageItem.setContentType(contentType);

                Map<String, Object> metaData = storageItem.getMetadata();
                if (metaData == null) {
                    metaData = new HashMap<String, Object>();
                }
                metaData.put("height", height);
                metaData.put("width", width);
                newStorageItem.setMetadata(metaData);
                newStorageItem.setStorage("");
                newStorageItem.setPath(path.toString());
                return newStorageItem;
            }
        } catch (MalformedURLException ex) {
            LOGGER.error(ex.getMessage(), ex);
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }

        return storageItem;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        //To Do
    }

}
