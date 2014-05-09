package com.psddev.dari.util;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import javax.imageio.ImageIO;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RoutingFilter.Path(application = "", value = "/dims4")
public class LocalImageServlet extends HttpServlet {

    public static final String LEGACY_PATH = "/dims4/";

    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        String[] urlAttributes = request.getServletPath().split("/http");
        String imageUrl = "";
        if (urlAttributes.length == 1  & request.getParameter("url") != null) {
            imageUrl = request.getParameter("url");
        } else if (urlAttributes.length == 2) {
            imageUrl = "http" + urlAttributes[1];
            if (!imageUrl.contains("://") && imageUrl.contains(":/")) {
                imageUrl = imageUrl.replace(":/", "://");
            }
        }

        if (!StringUtils.isBlank(imageUrl)) {
            String[] parameters = urlAttributes[0].substring(LEGACY_PATH.length()).split("/");

            BufferedImage bufferedImage = ImageIO.read(new URL(imageUrl));
            Date downloadDate = new Date();

            for (int i = 0; i < parameters.length; i = i + 2) {
                String command = parameters[i];
                String value = parameters[i + 1];

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
                        value = value.substring(0, value.length()-1);
                    }

                    String[] wh = value.split("x");
                    width = parseInteger(wh[0]);
                    if (wh.length == 2) {
                        height = parseInteger(wh[1]);
                    }

                    bufferedImage = LocalImageEditor.Resize(bufferedImage, width, height, option);
                    
                } else if (command.equals(ImageEditor.CROP_COMMAND)) {
                    String[] xywh = value.split("x");
                    bufferedImage = LocalImageEditor.Crop(bufferedImage, parseInteger(xywh[0]), parseInteger(xywh[1]), parseInteger(xywh[2]), parseInteger(xywh[3]));
                }
            }

            String imageType = "png";
            if (imageUrl.endsWith(".jpg") || imageUrl.endsWith(".jpeg")) {
                imageType = "jpg";
            } else if (imageUrl.endsWith(".gif")) {
                imageType = "gif";
            }

            response.setContentType("image/" + imageType);
            ServletOutputStream out = response.getOutputStream();
            ImageIO.write(bufferedImage, imageType, out);
           
            out.close();
        } else {
            response.getWriter().write("error");
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
