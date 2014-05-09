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
import javax.servlet.http.HttpServletRequest;
import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalImageEditor extends AbstractImageEditor {

    private static final String DEFAULT_IMAGE_FORMAT = "png";
    private static final String DEFAULT_IMAGE_CONTENT_TYPE = "image/" + DEFAULT_IMAGE_FORMAT;
    protected static final String THUMBNAIL_COMMAND = "thumbnail";

    protected static final Logger LOGGER = LoggerFactory.getLogger(LocalImageEditor.class);

    @Override
    public StorageItem edit(StorageItem storageItem, String command, Map<String, Object> options, Object... arguments) {
        BufferedImage bufferedImage = null;
        ByteArrayOutputStream ouputStream = new ByteArrayOutputStream();

        try {
            StringBuilder imageUrl = new StringBuilder();
            if (storageItem.getPublicUrl().startsWith("http") || PageContextFilter.Static.getRequest() == null) {
                imageUrl.append(storageItem.getPublicUrl());
            } else {
                HttpServletRequest request = PageContextFilter.Static.getRequest();

                imageUrl.append("http");
                if (request.isSecure()) {
                    imageUrl.append("s");
                }
                imageUrl.append("://");
                imageUrl.append(request.getServerName());

                if (request.getServerPort() != 80 && request.getServerPort() != 443 ) {
                    imageUrl.append(":")
                            .append(request.getServerPort());
                }
                imageUrl.append(storageItem.getPublicUrl());

            }

            if (storageItem.getPublicUrl().endsWith("tif") || storageItem.getPublicUrl().endsWith("tiff")) {
                //TODO add support for tif
            } else {
                bufferedImage = ImageIO.read(new URL(imageUrl.toString()));
            }

            if (bufferedImage == null) {
                 LOGGER.error("can't read image " + imageUrl.toString());
            }

            if (bufferedImage != null) {
                Object cropOption = options != null ? options.get(ImageEditor.CROP_OPTION) : null;

                if (ImageEditor.CROP_COMMAND.equals(command) &&
                        options != null &&
                        options.containsKey(ImageEditor.CROP_OPTION) &&
                        options.get(ImageEditor.CROP_OPTION).equals(ImageEditor.CROP_OPTION_NONE) ) {
                    return storageItem;
                }

                Integer width = null;
                Integer height = null;

                String url = storageItem.getPublicUrl();

                if (url.contains("/" + THUMBNAIL_COMMAND + "/")) {
                    return storageItem;
                }

                boolean newLocalImage = !url.contains(LocalImageServlet.LEGACY_PATH);

                StringBuilder path = new StringBuilder();
                if (newLocalImage) {
                    path.append(LocalImageServlet.LEGACY_PATH);
                } else {
                    path.append("/");
                }
                
                if (ImageEditor.CROP_COMMAND.equals(command) &&
                        ObjectUtils.to(Integer.class, arguments[0]) == null  &&
                        ObjectUtils.to(Integer.class, arguments[1]) == null) {
                    path.append(THUMBNAIL_COMMAND);
                    command = RESIZE_COMMAND;
                    arguments[0] = arguments[2];
                    arguments[1] = arguments[3];
                } else {
                    path.append(command);
                }
                path.append("/");

                if (ImageEditor.CROP_COMMAND.equals(command)) {
                    Integer x = ObjectUtils.to(Integer.class, arguments[0]);
                    Integer y = ObjectUtils.to(Integer.class, arguments[1]);
                    width = ObjectUtils.to(Integer.class, arguments[2]);
                    height = ObjectUtils.to(Integer.class, arguments[3]);
                    bufferedImage = Crop(bufferedImage, x, y, width, height);

                    path.append(x)
                        .append("x")
                        .append(y)
                        .append("x")
                        .append(width)
                        .append("x")
                        .append(height);

                } else if (ImageEditor.RESIZE_COMMAND.equals(command)) {
                    width = ObjectUtils.to(Integer.class, arguments[0]);
                    height = ObjectUtils.to(Integer.class, arguments[1]);
                    bufferedImage = Resize(bufferedImage, width, height, null);

                    if (width != null) {
                        path.append(width);
                    }
                    path.append("x");
                    if (height != null) {
                        path.append(height);
                    }
                    Object resizeOption = options != null ? options.get(ImageEditor.RESIZE_OPTION) : null;
                    
                    if (resizeOption != null && 
                            (cropOption == null || !cropOption.equals(ImageEditor.CROP_OPTION_AUTOMATIC)) ) {
                        if (resizeOption.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO)) {
                            path.append("!");
                        } else if (resizeOption.equals(ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER)) {
                            path.append(">");
                        } else if (resizeOption.equals(ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER)) {
                            path.append("<");
                        } else if (resizeOption.equals(ImageEditor.RESIZE_OPTION_FILL_AREA)) {
                            path.append("^");
                        }
                    }

                }

                if (newLocalImage) {
                    path.append("?url=").append(storageItem.getPublicUrl());
                } else {
                    String[] imageParameters = storageItem.getPublicUrl().split("\\?");
                    path.insert(0, imageParameters[0])
                        .append("?")
                        .append(imageParameters[1]);
                }

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

    /** Helper class so that width and height can be returned in a single object */
    protected static class Dimension {
        public final Integer width;
        public final Integer height;
        public Dimension(Integer width, Integer height) {
            this.width = width;
            this.height = height;
        }
    }

    public static BufferedImage Resize(BufferedImage bufferedImage, Integer width, Integer height, String option) {

        if (width != null || height != null) {
            if (!StringUtils.isBlank(option) &&
                    option.equals(ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER)) {
                if ((height == null && width >= bufferedImage.getWidth()) ||
                            (width == null && height >= bufferedImage.getHeight()) ||
                            (width != null && height != null && width >= bufferedImage.getWidth() && height >= bufferedImage.getHeight())) {
                    return bufferedImage;
                }
                
            } else if (!StringUtils.isBlank(option) &&
                    option.equals(ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER)) {
                if ((height == null && width <= bufferedImage.getWidth()) ||
                            (width == null && height <= bufferedImage.getHeight()) ||
                            (width != null && height != null && (width <= bufferedImage.getWidth() || height <= bufferedImage.getHeight()))) {
                    return bufferedImage;
                }
            }

            if (StringUtils.isBlank(option) ||
                    option.equals(ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER) ||
                    option.equals(ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER)) {
                if (height == null) {
                    return Scalr.resize(bufferedImage, Scalr.Mode.FIT_TO_WIDTH, width);
                } else if (width == null) {
                    return Scalr.resize(bufferedImage, Scalr.Mode.FIT_TO_HEIGHT, height);
                } else {
                    return Scalr.resize(bufferedImage, width, height);
                }
                
            } else if (height != null && width != null) {
                if (option.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO)) {
                    return Scalr.resize(bufferedImage, Scalr.Mode.FIT_EXACT, width, height);
                } else if (option.equals(ImageEditor.RESIZE_OPTION_FILL_AREA)) {
                    Dimension dimension = getFillAreaDimension(bufferedImage.getWidth(), bufferedImage.getHeight(), width, height);
                    return Scalr.resize(bufferedImage, Scalr.Mode.FIT_EXACT, dimension.width, dimension.height);
                }
                
            }

        }
        return null;
    }

    public static BufferedImage Crop(BufferedImage bufferedImage, Integer x, Integer y, Integer width, Integer height) {

        if (width != null || height != null) {
            if (height == null) {
                height = (int) ((double) bufferedImage.getHeight() / (double) bufferedImage.getWidth() * (double) width);
            } else if (width == null) {
                width = (int) ((double) bufferedImage.getWidth() / (double) bufferedImage.getHeight() * (double) height);
            }

            if (x == null) {
                x = bufferedImage.getWidth() / 2;
            }

            if (y == null) {
                y = bufferedImage.getHeight() / 2;
            }

            return Scalr.crop(bufferedImage, x, y, width, height);
        }

        return null;
    }

    private static Dimension getFillAreaDimension(Integer originalWidth, Integer originalHeight, Integer requestedWidth, Integer requestedHeight) {
        Integer actualWidth = null;
        Integer actualHeight = null;

        if (originalWidth != null && originalHeight != null &&
                (requestedWidth != null || requestedHeight != null)) {

            float originalRatio = (float) originalWidth / (float) originalHeight;
            if (requestedWidth != null && requestedHeight != null) {

                Integer potentialWidth = Math.round((float) requestedHeight * originalRatio);
                Integer potentialHeight = Math.round((float) requestedWidth / originalRatio);

                if (potentialWidth > requestedWidth) {
                    actualWidth = potentialWidth;
                    actualHeight = requestedHeight;

                } else { // potentialHeight > requestedHeight
                    actualWidth = requestedWidth;
                    actualHeight = potentialHeight;
                }

            } else if (originalWidth > originalHeight) {
                actualHeight = requestedHeight != null ? requestedHeight : requestedWidth;
                actualWidth = Math.round((float) actualHeight * originalRatio);

            } else { // originalWidth <= originalHeight
                actualWidth = requestedWidth != null ? requestedWidth : requestedHeight;
                actualHeight = Math.round((float) actualWidth / originalRatio);
            }
        }

        return new Dimension(actualWidth, actualHeight);
    }

}
