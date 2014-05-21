package com.psddev.dari.util;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.imgscalr.Scalr;

public class LocalImageEditor extends AbstractImageEditor {

    private static final String DEFAULT_IMAGE_FORMAT = "png";
    private static final String DEFAULT_IMAGE_CONTENT_TYPE = "image/" + DEFAULT_IMAGE_FORMAT;
    private static final String ORIGINAL_WIDTH_METADATA_PATH = "image/originalWidth";
    private static final String ORIGINAL_HEIGHT_METADATA_PATH = "image/originalHeight";

    /** Setting key for quality to use for the output images. */
    private static final String QUALITY_SETTING = "quality";

    protected static final String TIFF_READER_CLASS = "com.sun.media.imageioimpl.plugins.tiff.TIFFImageReaderSpi";
    protected static final String THUMBNAIL_COMMAND = "thumbnail";

    private Scalr.Method quality = Scalr.Method.AUTOMATIC;
    private String baseUrl;
    private String basePath;
    private String sharedSecret;


    public Scalr.Method getQuality() {
        return quality;
    }

    public void setQuality(Scalr.Method quality) {
        this.quality = quality;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public String getBasePath() {
        if (StringUtils.isBlank(basePath) && !(StringUtils.isBlank(baseUrl))) {
            basePath = baseUrl.substring(baseUrl.indexOf("//") + 2);
            basePath = basePath.substring(basePath.indexOf("/") + 1);
            if (!basePath.endsWith("/")) {
                basePath += "/";
            }
        }
        return basePath;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getSharedSecret() {
        return sharedSecret;
    }

    public void setSharedSecret(String sharedSecret) {
        this.sharedSecret = sharedSecret;
    }

    @Override
    public StorageItem edit(StorageItem storageItem, String command, Map<String, Object> options, Object... arguments) {

        if (StringUtils.isBlank(this.getBasePath()) && PageContextFilter.Static.getRequest() != null) {
            setBaseUrlFromRequest(PageContextFilter.Static.getRequest());
        }

        if (ImageEditor.CROP_COMMAND.equals(command) &&
                options != null &&
                options.containsKey(ImageEditor.CROP_OPTION) &&
                options.get(ImageEditor.CROP_OPTION).equals(ImageEditor.CROP_OPTION_NONE)) {
            return storageItem;
        }

        String imageUrl = storageItem.getPublicUrl();
        List<String> commands = new ArrayList<String>();

        if (imageUrl.startsWith(this.getBaseUrl()) && imageUrl.contains("?url=")) {
            String[] imageComponents = imageUrl.split("\\?url=");
            imageUrl = imageComponents[1];

            String path = imageComponents[0].substring(this.getBaseUrl().length());
            for (String parameter : path.split("/")) {
                commands.add(parameter);
            }

            if (!StringUtils.isBlank(this.getSharedSecret())) {
                commands = commands.subList(2, commands.size());
            }
        }

        Object cropOption = options != null ? options.get(ImageEditor.CROP_OPTION) : null;

        Dimension originalDimension = null;
        Dimension outputDimension = null;

        Map<String, Object> oldMetadata = storageItem.getMetadata();
        if (oldMetadata != null) {
            Integer originalWidth = null;
            Integer originalHeight = null;
            // grab the original width and height of the image
            originalWidth = ObjectUtils.to(Integer.class,
                    CollectionUtils.getByPath(oldMetadata, ORIGINAL_WIDTH_METADATA_PATH));
            if (originalWidth == null) {
                originalWidth = ObjectUtils.to(Integer.class, oldMetadata.get("width"));
            }

            originalHeight = ObjectUtils.to(Integer.class,
                    CollectionUtils.getByPath(oldMetadata, ORIGINAL_HEIGHT_METADATA_PATH));
            if (originalHeight == null) {
                originalHeight = ObjectUtils.to(Integer.class, oldMetadata.get("height"));
            }
            originalDimension = new Dimension(originalWidth, originalHeight);
        }

        if (ImageEditor.CROP_COMMAND.equals(command) &&
                        ObjectUtils.to(Integer.class, arguments[0]) == null  &&
                        ObjectUtils.to(Integer.class, arguments[1]) == null) {
            commands.add(THUMBNAIL_COMMAND);
            command = RESIZE_COMMAND;
            arguments[0] = arguments[2];
            arguments[1] = arguments[3];
        } else {
            commands.add(command);
        }

        if (ImageEditor.CROP_COMMAND.equals(command)) {
            Integer width = ObjectUtils.to(Integer.class, arguments[2]);
            Integer height = ObjectUtils.to(Integer.class, arguments[3]);

            commands.add(arguments[0] + "x" + arguments[1] + "x" + width + "x" + height);
            if (originalDimension != null) {
                outputDimension = new Dimension(Math.min(originalDimension.width, width),
                                                Math.min(originalDimension.height, height));
            }

        } else if (ImageEditor.RESIZE_COMMAND.equals(command)) {
            Integer width = ObjectUtils.to(Integer.class, arguments[0]);
            Integer height = ObjectUtils.to(Integer.class, arguments[1]);

            StringBuilder resizeBuilder = new StringBuilder();
            if (width != null) {
                resizeBuilder.append(width);
            }
            resizeBuilder.append("x");
            if (height != null) {
                resizeBuilder.append(height);
            }
            Object resizeOption = options != null ? options.get(ImageEditor.RESIZE_OPTION) : null;

            if (resizeOption != null &&
                    (cropOption == null || !cropOption.equals(ImageEditor.CROP_OPTION_AUTOMATIC))) {
                if (resizeOption.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO)) {
                    resizeBuilder.append("!");
                } else if (resizeOption.equals(ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER)) {
                    resizeBuilder.append(">");
                    if (originalDimension != null && width != null && height != null) {
                        outputDimension = new Dimension(Math.min(originalDimension.width, width),
                                                        Math.min(originalDimension.height, height));
                    }
                } else if (resizeOption.equals(ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER)) {
                    resizeBuilder.append("<");
                    if (originalDimension != null && width != null && height != null) {
                        outputDimension = new Dimension(Math.max(originalDimension.width, width),
                                                        Math.max(originalDimension.height, height));
                    }
                } else if (resizeOption.equals(ImageEditor.RESIZE_OPTION_FILL_AREA)) {
                    resizeBuilder.append("^");
                }
            }

            if (originalDimension != null && width != null && height != null && (resizeOption == null ||
                    resizeOption.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO) ||
                    resizeOption.equals(ImageEditor.RESIZE_OPTION_FILL_AREA))) {
                outputDimension = new Dimension(Math.min(originalDimension.width, width),
                                                Math.min(originalDimension.height, height));
            }
            commands.add(resizeBuilder.toString());

        }

        StringBuilder storageItemUrlBuilder = new StringBuilder();
        storageItemUrlBuilder.append(this.getBaseUrl());
        if (!StringUtils.isBlank(this.getSharedSecret())) {
            StringBuilder commandsBuilder = new StringBuilder();

            for (String parameter : commands) {
                commandsBuilder.append(StringUtils.encodeUri(parameter))
                               .append('/');
            }

            Long expireTs = (long) Integer.MAX_VALUE;
            String signature = expireTs + this.getSharedSecret() + StringUtils.decodeUri("/" + commandsBuilder.toString()) + imageUrl;

            String md5Hex = StringUtils.hex(StringUtils.md5(signature));
            String requestSig = md5Hex.substring(0, 7);

            storageItemUrlBuilder.append(requestSig)
                .append("/")
                .append(expireTs.toString())
                .append("/");
        }

        for (String parameter : commands) {
            storageItemUrlBuilder.append(parameter)
                .append("/");
        }

        storageItemUrlBuilder.append("?url=")
                .append(imageUrl);

        UrlStorageItem newStorageItem = StorageItem.Static.createUrl(storageItemUrlBuilder.toString());
        String format = DEFAULT_IMAGE_FORMAT;
        String contentType = DEFAULT_IMAGE_CONTENT_TYPE;
        if (storageItem.getContentType() != null && storageItem.getContentType().contains("/")) {
            contentType = storageItem.getContentType();
            format = storageItem.getContentType().split("/")[1];
        }

        newStorageItem.setContentType(contentType);

        Map<String, Object> metadata = storageItem.getMetadata();
        if (metadata == null) {
            metadata = new HashMap<String, Object>();
        }

        // store the new width and height in the metadata map
        if (outputDimension != null && outputDimension.width != null) {
            metadata.put("width", outputDimension.width);
        }
        if (outputDimension != null && outputDimension.height != null) {
            metadata.put("height", outputDimension.height);
        }

        // store the original width and height in the map for use with future image edits.
        if (originalDimension != null && originalDimension.width != null) {
            CollectionUtils.putByPath(metadata, ORIGINAL_WIDTH_METADATA_PATH, originalDimension.width);
        }
        if (originalDimension != null && originalDimension.height != null) {
            CollectionUtils.putByPath(metadata, ORIGINAL_HEIGHT_METADATA_PATH, originalDimension.height);
        }

        newStorageItem.setMetadata(metadata);

        return newStorageItem;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        Object qualitySetting = settings.get(QUALITY_SETTING);
        if (qualitySetting == null) {
            qualitySetting = Settings.get(QUALITY_SETTING);
        }

        if (qualitySetting != null) {
            if (qualitySetting instanceof Integer) {
                Integer qualityInteger = ObjectUtils.to(Integer.class, qualitySetting);
                quality = findQualityByInteger(qualityInteger);
            } else if (qualitySetting instanceof String) {
                quality = Scalr.Method.valueOf(ObjectUtils.to(String.class, qualitySetting));
            }
        }

        if (!ObjectUtils.isBlank(settings.get("servletPath"))) {
            LocalImageServlet.setServletPath(ObjectUtils.to(String.class, settings.get("servletPath")));
        }

        if (!ObjectUtils.isBlank(settings.get("baseUrl"))) {
            setBaseUrl(ObjectUtils.to(String.class, settings.get("baseUrl")));
        }

        if (!ObjectUtils.isBlank(settings.get("sharedSecret"))) {
            setSharedSecret(ObjectUtils.to(String.class, settings.get("sharedSecret")));
        }
    }

    protected void setBaseUrlFromRequest(HttpServletRequest request) {

        StringBuilder baseUrlBuilder = new StringBuilder();
        baseUrlBuilder.append("http");
        if (request.isSecure()) {
            baseUrlBuilder.append("s");
        }

        baseUrlBuilder.append("://")
                .append(request.getServerName());

        if (request.getServerPort() != 80 && request.getServerPort() != 443) {
            baseUrlBuilder.append(":")
                    .append(request.getServerPort());
        }

        baseUrlBuilder.append(LocalImageServlet.getServletPath());
        setBaseUrl(baseUrlBuilder.toString());
    }

    protected static Scalr.Method findQualityByInteger(Integer quality) {
        if (quality >= 80) {
            return Scalr.Method.ULTRA_QUALITY;
        } else if (quality >= 60) {
            return Scalr.Method.QUALITY;
        } else if (quality >= 40) {
            return Scalr.Method.AUTOMATIC;
        } else if (quality >= 20) {
            return Scalr.Method.BALANCED;
        } else {
            return Scalr.Method.SPEED;
        }
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

    public BufferedImage reSize(BufferedImage bufferedImage, Integer width, Integer height, String option, Scalr.Method quality) {
        if (quality == null) {
            quality = this.quality;
        }
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
                    return Scalr.resize(bufferedImage, quality, Scalr.Mode.FIT_TO_WIDTH, width);
                } else if (width == null) {
                    return Scalr.resize(bufferedImage, quality, Scalr.Mode.FIT_TO_HEIGHT, height);
                } else {
                    return Scalr.resize(bufferedImage, quality, width, height);
                }

            } else if (height != null && width != null) {
                if (option.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO)) {
                    return Scalr.resize(bufferedImage, quality, Scalr.Mode.FIT_EXACT, width, height);
                } else if (option.equals(ImageEditor.RESIZE_OPTION_FILL_AREA)) {
                    Dimension dimension = getFillAreaDimension(bufferedImage.getWidth(), bufferedImage.getHeight(), width, height);
                    return Scalr.resize(bufferedImage, quality, Scalr.Mode.FIT_EXACT, dimension.width, dimension.height);
                }

            }

        }
        return null;
    }

    public static BufferedImage crop(BufferedImage bufferedImage, Integer x, Integer y, Integer width, Integer height) {

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

            if (x + width > bufferedImage.getWidth()) {
                width = bufferedImage.getWidth() - x;
            }

            if (y + height > bufferedImage.getHeight()) {
                height = bufferedImage.getHeight() - y;
            }

            return Scalr.crop(bufferedImage, x, y, width, height);
        }

        return null;
    }

    public static BufferedImage grayscale(BufferedImage sourceImage) {
        BufferedImage resultImage = new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), BufferedImage.TYPE_BYTE_GRAY);
        Graphics g = resultImage.getGraphics();
        g.drawImage(sourceImage, 0, 0, null);
        g.dispose();
        return resultImage;
    }

    public static BufferedImage brightness(BufferedImage sourceImage, int brightness, int contrast) {
        BufferedImage resultImage =  new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());

        int multiply = 100;
        int add;

        if (contrast == 0) {
            add = Math.round(brightness / 100.0f * 255);
        } else {
            if (contrast > 0) {
                contrast = contrast * 4;
            }
            contrast = 100 - (contrast * -1);
            multiply = contrast;
            brightness = Math.round(brightness / 100.0f * 255);

            add = ((Double) (((brightness - 128) * (multiply / 100.0d) + 128))).intValue();

        }

        for (int x = 0; x < sourceImage.getWidth(); x++) {
            for (int y = 0; y < sourceImage.getHeight(); y++) {
                int rgb = sourceImage.getRGB(x, y);
                int alpha = (rgb >> 24) & 0xFF;
                int red   = (rgb >> 16) & 0xFF;
                int green = (rgb >> 8) & 0xFF;
                int blue  = rgb & 0xFF;

                red = adjustColor(red, multiply, add);
                green = adjustColor(green, multiply, add);
                blue = adjustColor(blue, multiply, add);

                int newRgb = (alpha << 24) | (red << 16) | (green << 8) | blue;

                resultImage.setRGB(x, y, newRgb);
            }
        }

        return resultImage;
    }

    public static BufferedImage flipHorizontal(BufferedImage sourceImage) {
        return Scalr.rotate(sourceImage, Scalr.Rotation.FLIP_HORZ);
    }

    public static BufferedImage flipVertical(BufferedImage sourceImage) {
        return Scalr.rotate(sourceImage, Scalr.Rotation.FLIP_VERT);
    }

    public static BufferedImage invert(BufferedImage sourceImage) {
        BufferedImage resultImage =  new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());

        for (int x = 0; x < sourceImage.getWidth(); x++) {
            for (int y = 0; y < sourceImage.getHeight(); y++) {
                int rgb = sourceImage.getRGB(x, y);
                int alpha = (rgb >> 24) & 0xFF;
                int red   = 255 - (rgb >> 16) & 0xFF;
                int green = 255 - (rgb >> 8) & 0xFF;
                int blue  = 255 - rgb & 0xFF;

                int newRgb = (alpha << 24) | (red << 16) | (green << 8) | blue;

                resultImage.setRGB(x, y, newRgb);
            }
        }

        return resultImage;
    }

    public static BufferedImage rotate(BufferedImage sourceImage, int degrees) {
        Scalr.Rotation rotation;
        if (degrees == 90) {
            rotation = Scalr.Rotation.CW_90;
        } else if (degrees == 180) {
            rotation = Scalr.Rotation.CW_180;
        } else if (degrees == 270 || degrees == -90) {
            rotation = Scalr.Rotation.CW_270;
        } else {
            double radians = Math.toRadians(degrees);
            int w = (new Double(Math.abs(sourceImage.getWidth() * Math.cos(radians)) + Math.abs(sourceImage.getHeight() * Math.sin(radians)))).intValue();
            int h = (new Double(Math.abs(sourceImage.getWidth() * Math.sin(radians)) + Math.abs(sourceImage.getHeight() * Math.cos(radians)))).intValue();

            BufferedImage resultImage = new BufferedImage(w, h, sourceImage.getType());
            Graphics2D graphics = resultImage.createGraphics();

            graphics.setColor(Color.WHITE);
            graphics.fillRect(0, 0, w, h);

            int x = -1 * (sourceImage.getWidth() - w) / 2;
            int y = -1 * (sourceImage.getHeight() - h) / 2;

            graphics.rotate(Math.toRadians(degrees), (w / 2), (h / 2));
            graphics.drawImage(sourceImage, null, x, y);
            return resultImage;
        }
        return Scalr.rotate(sourceImage, rotation);
    }

    public static BufferedImage sepia(BufferedImage sourceImage) {
        BufferedImage resultImage = new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());

        for (int x = 0; x < sourceImage.getWidth(); x++) {
            for (int y = 0; y < sourceImage.getHeight(); y++) {
                int rgb = sourceImage.getRGB(x, y);
                int alpha = (rgb >> 24) & 0xFF;
                int red   = (rgb >> 16) & 0xFF;
                int green = (rgb >> 8) & 0xFF;
                int blue  = rgb & 0xFF;

                int newRed = (new Double((red * .393) + (green * .769) + (blue * .189))).intValue();
                int newGreen = (new Double((red * .349) + (green * .686) + (blue * .168))).intValue();
                int newBlue = (new Double((red * .272) + (green * .534) + (blue * .131))).intValue();

                newRed = colorMinMax(newRed);
                newGreen = colorMinMax(newGreen);
                newBlue = colorMinMax(newBlue);

                int newRgb = (alpha << 24) | (newRed << 16) | (newGreen << 8) | newBlue;

                resultImage.setRGB(x, y, newRgb);
            }
        }

        return resultImage;
    }

    private static int adjustColor(int color, int multiply, int add) {
        color =  Math.round(color * (multiply / 100.0f)) + add;
        return colorMinMax(color);
    }

    private static int colorMinMax(int color) {
        if (color < 0) {
            return 0;
        } else if (color > 255) {
            return 255;
        }
        return color;
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
