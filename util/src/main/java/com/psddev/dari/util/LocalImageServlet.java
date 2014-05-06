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

        Date startDate = new Date();

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
                    Integer width = null;
                    Integer height = null;
                    String[] wh = value.split("x");
                    width = wh[0].equals("") ? null : Integer.parseInt(wh[0]);
                    if (wh.length == 2) {
                        height = Integer.parseInt(wh[1]);
                    }

                    bufferedImage = LocalImage.Resize(bufferedImage, width, height);
                } else if (command.equals(ImageEditor.CROP_COMMAND)) {
                    String[] xywh = value.split("x");
                    bufferedImage = LocalImage.Crop(bufferedImage, Integer.parseInt(xywh[0]), Integer.parseInt(xywh[1]), Integer.parseInt(xywh[2]), Integer.parseInt(xywh[3]));
                }
            }

            Date resizeDate = new Date();

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
}
