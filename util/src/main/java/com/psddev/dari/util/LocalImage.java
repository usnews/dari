package com.psddev.dari.util;

import java.awt.image.BufferedImage;
import org.imgscalr.Scalr;

public class LocalImage {

    private static final String DEFAULT_IMAGE_FORMAT = "png";
    private static final String DEFAULT_IMAGE_CONTENT_TYPE = "image/" + DEFAULT_IMAGE_FORMAT;

    public static BufferedImage Resize(BufferedImage bufferedImage, Integer width, Integer height) {

        if (width != null || height != null) {
            if (height == null) {
                height = (int) ((double) bufferedImage.getHeight() / (double) bufferedImage.getWidth() * (double) width);
            } else if (width == null) {
                width = (int) ((double) bufferedImage.getWidth() / (double) bufferedImage.getHeight() * (double) height);
            }
            return Scalr.resize(bufferedImage, width, height);
        }
        return null;
    }

    public static BufferedImage Crop(BufferedImage bufferedImage, Integer x, Integer y, Integer width, Integer height) {
        return Scalr.crop(bufferedImage, x, y, width, height);
    }
}
