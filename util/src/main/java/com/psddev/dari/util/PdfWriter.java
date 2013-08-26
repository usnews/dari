package com.psddev.dari.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;
import org.apache.fop.apps.FOPException;
import org.apache.fop.apps.Fop;
import org.apache.fop.apps.FopFactory;
import org.apache.xmlgraphics.util.MimeConstants;

/**
 * Processes XSL-FO returning a PDF with {@link #writePdf}<br/>
 * XSL example of HTML to XSL-FO <a href="http://catcode.com/cis97yt/xslfo.html">http://catcode.com/cis97yt/xslfo.html</a>
 */
public class PdfWriter {
    
    private static FopFactory FOP_FACTORY = null;

    private static FopFactory getFopFactory() {
        if(FOP_FACTORY == null) {
            FOP_FACTORY = FopFactory.newInstance();
        }
        return FOP_FACTORY;
    }


    /**
     * Service to generate a PDF<br/>
     * Default to using request attribute mainContent that implements PdfWriter<br/>
     * Optional key/value parameter pairs fileName, xml, xsl and pdfWriteable
     * @param request
     * @param response
     * @param parameters
     * @throws IOException
     * @throws ServletException
     * @throws FOPException
     * @throws TransformerConfigurationException
     * @throws TransformerException
     */
    public static void writePdf(HttpServletRequest request, HttpServletResponse response, Object... parameters)
            throws IOException, ServletException, FOPException, TransformerConfigurationException, TransformerException {

        String xml = null;
        String xsl = null;
        String fileName = null;

        for(int i = 0; i < parameters.length; i += 2) {
            String key = (String)parameters[i];
            if(key.equals("xml")) {
                xml = (String)parameters[i+1];
            }else if(key.equals("xsl")) {
                xsl = (String)parameters[i+1];
            }else if(key.equals("fileName")) {
                fileName = (String)parameters[i+1];
            }else if(key.equals("pdfWriteable")) {
                PdfWriteable pdfWriteable =  (PdfWriteable)parameters[i+1];
                xml = pdfWriteable.getPdfXml();
                xsl = pdfWriteable.getPdfXsl();
            }
        }

        if(request.getAttribute("mainContent") != null && request.getAttribute("mainContent") instanceof PdfWriteable) {
            PdfWriteable pdfWriteable = (PdfWriteable)request.getAttribute("mainContent");
            if(StringUtils.isBlank(xml)) {
                xml = pdfWriteable.getPdfXml();
            }
            if(StringUtils.isBlank(xsl)) {
                xsl = pdfWriteable.getPdfXsl();
            }
        }

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {
            FopFactory fopFactory = PdfWriter.getFopFactory();
            Fop fop = fopFactory.newFop(MimeConstants.MIME_PDF, outStream);

            //Setup Transformer
            TransformerFactory tFactory = TransformerFactory.newInstance();
            Transformer transformer;
            if(StringUtils.isBlank(xsl)) {
                transformer = tFactory.newTransformer(); // identity transformer
            } else{
                Source xsltSrc = new StreamSource(new StringReader(xsl));
                transformer = tFactory.newTransformer(xsltSrc);
            }

            //Make sure the XSL transformation's result is piped through to FOP
            Result res = new SAXResult(fop.getDefaultHandler());

            //Setup input
            Source src = new StreamSource(new StringReader(xml));

            if (ObjectUtils.to(boolean.class, request.getParameter(DebugFilter.DEBUG_PARAMETER))) {
                StringWriter writer = new StringWriter();
                Result result = new javax.xml.transform.stream.StreamResult(writer);
                transformer.transform(src, result);
                response.getWriter().write(StringUtils.escapeHtml(writer.toString()));
            }else {
                //Start the transformation and rendering process
                transformer.transform(src, res);

                //Prepare response
                if(StringUtils.isBlank(fileName)) {
                    response.setContentType("application/pdf");
                }else {
                    if(!fileName.toLowerCase().endsWith(".pdf")) {
                        fileName += ".pdf";
                    }
                    response.setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
                    response.setContentType("application/octet-stream");
                }
                response.setContentLength(outStream.size());

                //Send content to Browser
                response.getOutputStream().write(outStream.toByteArray());
                response.getOutputStream().flush();
            }

        }finally {
            outStream.close();
        }
    }
}
