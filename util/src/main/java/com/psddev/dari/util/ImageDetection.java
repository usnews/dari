package com.psddev.dari.util;
//import com.googlecode.javacv.cpp.opencv_core.IplImage;

import java.awt.image.BufferedImage;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import javax.imageio.ImageIO;
import org.bytedeco.javacpp.Loader;
import static org.bytedeco.javacpp.helper.opencv_objdetect.cvHaarDetectObjects;
import org.bytedeco.javacpp.opencv_core.CvMemStorage;
import org.bytedeco.javacpp.opencv_core.CvRect;
import org.bytedeco.javacpp.opencv_core.CvSeq;
import static org.bytedeco.javacpp.opencv_core.IPL_DEPTH_8U;
import org.bytedeco.javacpp.opencv_core.IplImage;
import static org.bytedeco.javacpp.opencv_core.cvClearMemStorage;
import static org.bytedeco.javacpp.opencv_core.cvGetSeqElem;
import static org.bytedeco.javacpp.opencv_core.cvLoad;
import static org.bytedeco.javacpp.opencv_imgproc.CV_BGR2GRAY;
import static org.bytedeco.javacpp.opencv_imgproc.cvCvtColor;
import org.bytedeco.javacpp.opencv_objdetect;
import static org.bytedeco.javacpp.opencv_objdetect.CV_HAAR_DO_CANNY_PRUNING;
import org.bytedeco.javacpp.opencv_objdetect.CvHaarClassifierCascade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageDetection {
    private static CvHaarClassifierCascade classifier;
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFilter.class);

    public ImageDetection() {
        String classifierFile = this.getClass().getResource("/_resource/opencv/haarcascades/haarcascade_frontalface_alt.xml").getPath();
        Loader.load(opencv_objdetect.class);

        classifier = new CvHaarClassifierCascade(cvLoad(classifierFile));
        if (classifier.isNull()) {
            LOGGER.error("Error loading classifier file \"" + classifierFile + "\".");
        }
    }

    public List<CvRect> findFaces(String image) throws Exception {

        List<CvRect> faces = new ArrayList<CvRect>();

        BufferedImage bufferedImage = ImageIO.read(new URL(image));
        IplImage iplImage = IplImage.createFrom(bufferedImage);

        int width = iplImage.width();
        int height = iplImage.height();
        IplImage grayImage = IplImage.create(width, height, IPL_DEPTH_8U, 1);

        CvMemStorage storage = CvMemStorage.create();
        cvClearMemStorage(storage);
        cvCvtColor(iplImage, grayImage, CV_BGR2GRAY);

        CvSeq cvSeq = cvHaarDetectObjects(grayImage, classifier, storage, 1.1, 3, CV_HAAR_DO_CANNY_PRUNING);
        int total = cvSeq.total();

        for (int i = 0; i < total; i++) {
            CvRect r = new CvRect(cvGetSeqElem(cvSeq, i));
            faces.add(r);
        }

        return faces;
    }
}
