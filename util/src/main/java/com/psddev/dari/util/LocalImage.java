package com.psddev.dari.util;

import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;

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
            BufferedImage after = new BufferedImage(width, height, bufferedImage.getType());
            AffineTransform at = new AffineTransform();
            at.scale(((double) width / (double) bufferedImage.getWidth()), ((double) height / (double) bufferedImage.getHeight()));

            AffineTransformOp scaleOp = new AffineTransformOp(at, AffineTransformOp.TYPE_BILINEAR);
            after = scaleOp.filter(bufferedImage, after);

            return after;

        }
        return null;
    }

    public static BufferedImage Crop(BufferedImage bufferedImage, Integer x, Integer y, Integer width, Integer height) {
        BufferedImage newBufferedImage = bufferedImage.getSubimage(x, y, width, height);
        return newBufferedImage;
    }
}
