package com.psddev.dari.util;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import javax.imageio.ImageIO;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RoutingFilter.Path(application = "", value = "/dims4")
public class LocalImageServlet extends HttpServlet {
    private static final String QUALITY_OPTION = "quality";
    protected static final Logger LOGGER = LoggerFactory.getLogger(LocalImageEditor.class);
    public static final String LEGACY_PATH = "/dims4/";

    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        String[] urlAttributes = request.getServletPath().split("/http");
        String imageUrl = "";
        if (urlAttributes.length == 1 && request.getParameter("url") != null) {
            imageUrl = request.getParameter("url");
        } else if (urlAttributes.length == 2) {
            imageUrl = "http" + urlAttributes[1];
            if (!imageUrl.contains("://") && imageUrl.contains(":/")) {
                imageUrl = imageUrl.replace(":/", "://");
            }
        }

        if (!StringUtils.isBlank(imageUrl)) {

            String imageType = "png";
            if (imageUrl.endsWith(".jpg") || imageUrl.endsWith(".jpeg")) {
                imageType = "jpg";
            } else if (imageUrl.endsWith(".gif")) {
                imageType = "gif";
            }

            LocalImageEditor localImageEditor = ObjectUtils.to(LocalImageEditor.class, ImageEditor.Static.getInstance("local"));

            String[] parameters = null;
            if (!StringUtils.isBlank(localImageEditor.getBaseUrl())) {
                if (!urlAttributes[0].contains(localImageEditor.getBasePath())) {
                    response.sendError(404);
                    return;
                }
                parameters = urlAttributes[0].substring(localImageEditor.getBasePath().length() + 1).split("/");

                //Verify key
                if (!StringUtils.isBlank(localImageEditor.getSharedSecret())) {
                    StringBuilder commandsBuilder = new StringBuilder();
                    for (int i = 2; i < parameters.length; i++) {
                        commandsBuilder.append(StringUtils.encodeUri(parameters[i]))
                                       .append('/');
                    }

                    Long expireTs = (long) Integer.MAX_VALUE;
                    String signature = expireTs + localImageEditor.getSharedSecret() + StringUtils.decodeUri("/" + commandsBuilder.toString()) + imageUrl;

                    String md5Hex = StringUtils.hex(StringUtils.md5(signature));
                    String requestSig = md5Hex.substring(0, 7);

                    if (!parameters[0].equals(requestSig) || !parameters[1].equals(expireTs.toString())) {
                        response.sendError(404);
                        return;
                    }
                }
            } else {
                parameters = urlAttributes[0].substring(LEGACY_PATH.length()).split("/");
            }

            BufferedImage bufferedImage;
            if ((imageUrl.endsWith("tif") || imageUrl.endsWith("tiff")) && ObjectUtils.getClassByName(LocalImageEditor.TIFF_READER_CLASS) != null) {
                bufferedImage = LocalImageTiffReader.readTiff(imageUrl);
            } else {
                bufferedImage = ImageIO.read(new URL(imageUrl));
            }

            if (bufferedImage == null) {
                throw new IOException(String.format("Unable to process image %s", imageUrl));
            }

            Scalr.Method quality = null;
            for (int i = 0; i < parameters.length; i = i + 2) {
                String command = parameters[i];

                if (command.equals(QUALITY_OPTION)) {
                    String value = parameters[i + 1];
                    try {
                        quality = Scalr.Method.valueOf(Scalr.Method.class, value.toUpperCase());
                    } catch (IllegalArgumentException ex) {
                        quality = LocalImageEditor.findQualityByInteger(Integer.parseInt(value));
                    }
                }
            }

            for (int i = 0; i < parameters.length; i = i + 2) {
                String command = parameters[i];
                String value = i + 1 < parameters.length ? parameters[i + 1] : null;

                if (command.equals(ImageEditor.RESIZE_COMMAND)) {
                    String option = null;
                    Integer width = null;
                    Integer height = null;

                    if (value.endsWith("!")) {
                        option = ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO;
                    } else if (value.endsWith(">")) {
                        option = ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER;
                    } else if (value.endsWith("<")) {
                        option = ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER;
                    } else if (value.endsWith("^")) {
                        option = ImageEditor.RESIZE_OPTION_FILL_AREA;
                    }
                    if (option != null) {
                        value = value.substring(0, value.length() - 1);
                    }

                    String[] wh = value.split("x");
                    width = parseInteger(wh[0]);
                    if (wh.length == 2) {
                        height = parseInteger(wh[1]);
                    }

                    bufferedImage = localImageEditor.reSize(bufferedImage, width, height, option, quality);

                } else if (command.equals(ImageEditor.CROP_COMMAND)) {
                    Integer x = 0;
                    Integer y = 0;
                    Integer width = null;
                    Integer height = null;
                    String[] size;

                    if (value.contains("+")) {
                        int delimiter = value.indexOf("+");
                        String[] xy = value.substring(delimiter + 1).split("\\+");

                        x = parseInteger(xy[0]) != null ? parseInteger(xy[0]) : 0;
                        y = parseInteger(xy[1]) != null ? parseInteger(xy[1]) : 0;

                        size = value.substring(0, delimiter).split("x");

                    } else {
                        size = value.split("x");
                        if (size.length > 3) {
                            x = parseInteger(size[2]) != null ? parseInteger(size[2]) : 0;
                            y = parseInteger(size[3]) != null ? parseInteger(size[3]) : 0;
                        }
                    }

                    width = parseInteger(size[0]);
                    if (size.length > 1) {
                        height = parseInteger(size[1]);
                    }

                    bufferedImage = LocalImageEditor.crop(bufferedImage, x, y, width, height);

                } else if (command.equals(LocalImageEditor.THUMBNAIL_COMMAND)) {
                    String option = null;

                    if (value.endsWith("!")) {
                        option = ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO;
                    } else if (value.endsWith(">")) {
                        option = ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER;
                    } else if (value.endsWith("<")) {
                        option = ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER;
                    } else if (value.endsWith("^")) {
                        option = ImageEditor.RESIZE_OPTION_FILL_AREA;
                    }
                    if (option != null) {
                        value = value.substring(0, value.length() - 1);
                    }

                    String[] wh = value.split("x");
                    Integer width = ObjectUtils.to(Integer.class, wh[0]);
                    Integer height = ObjectUtils.to(Integer.class, wh[1]);

                    int resizeHeight =  height;
                    int resizeWidth = width;

                    if (option == null || !option.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO)) {
                        resizeHeight = (int) ((double) bufferedImage.getHeight() / (double) bufferedImage.getWidth() * (double) width);
                        resizeWidth  = (int) ((double) bufferedImage.getWidth() / (double) bufferedImage.getHeight() * (double) height);
                    }

                    bufferedImage = localImageEditor.reSize(bufferedImage, resizeWidth, resizeHeight, option, quality);
                    if ((width != bufferedImage.getWidth() || height != bufferedImage.getHeight())) {

                        //Allows for crop when reSized size is slightly off
                        if (width > bufferedImage.getWidth() && (width - 2) <= bufferedImage.getWidth()) {
                            width = bufferedImage.getWidth();
                        }

                        if (height > bufferedImage.getHeight() && (height - 2) <= bufferedImage.getHeight()) {
                            height = bufferedImage.getHeight();
                        }

                        if (width <= bufferedImage.getWidth() && height <= bufferedImage.getHeight()) {
                            bufferedImage = LocalImageEditor.crop(bufferedImage, 0, 0, width, height);
                        }
                    }
                } else if (command.equals("grayscale")) {
                    bufferedImage = LocalImageEditor.grayscale(bufferedImage);
                } else if (command.equals("brightness")) {
                    String[] wh = value.split("x");
                    Integer brightness = Integer.valueOf(wh[0]);
                    Integer contrast = Integer.valueOf(wh[1]);
                    bufferedImage = LocalImageEditor.brightness(bufferedImage, brightness, contrast);
                } else if (command.equals("flipflop")) {
                    if (value.equals("horizontal")) {
                        bufferedImage = LocalImageEditor.flipHorizontal(bufferedImage);
                    } else if (value.equals("vertical")) {
                        bufferedImage = LocalImageEditor.flipVertical(bufferedImage);
                    }
                } else if (command.equals("invert")) {
                    bufferedImage = LocalImageEditor.invert(bufferedImage);
                } else if (command.equals("rotate")) {
                    bufferedImage = LocalImageEditor.rotate(bufferedImage, Integer.valueOf(parameters[i + 1]));
                } else if (command.equals("sepia")) {
                    bufferedImage = LocalImageEditor.sepia(bufferedImage);
                } else if (command.equals("format")) {
                    imageType = value;
                }
            }

            response.setContentType("image/" + imageType);
            ServletOutputStream out = response.getOutputStream();
            ImageIO.write(bufferedImage, imageType, out);

            out.close();
        } else {
            throw new IOException("No source image provided");
        }
    }

    private Integer parseInteger(String integer) {
        if (StringUtils.isBlank(integer) || integer.matches("null")) {
            return null;
        } else {
            return Integer.parseInt(integer);
        }
    }
}
