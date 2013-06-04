package com.psddev.dari.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

/** Enables {@code multipart/form-data} handling. */
public class MultipartRequest extends HttpServletRequestWrapper {

    private final Map<String, List<FileItem>> parameters = new LinkedHashMap<String, List<FileItem>>();

    /** Creates an instance that wraps the given {@code request}. */
    public MultipartRequest(HttpServletRequest request) throws ServletException {
        super(request);

        try {
            ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
            upload.setHeaderEncoding("UTF-8");

            @SuppressWarnings("unchecked")
            List<FileItem> items = (List<FileItem>) upload.parseRequest(request);
            for (FileItem item : items) {
                String name = item.getFieldName();
                List<FileItem> values = parameters.get(name);
                if (values == null) {
                    values = new ArrayList<FileItem>();
                    parameters.put(name, values);
                }
                values.add(item);
            }

        } catch (FileUploadException ex) {
            throw new ServletException(ex);
        }
    }

    /** Returns the first file item associated with the given {@code name}. */
    public FileItem getFileItem(String name) {
        List<FileItem> values = parameters.get(name);
        return values == null || values.size() == 0 ? null : values.get(0);
    }

    /** Returns all file items associated with the given {@code name}. */
    public FileItem[] getFileItems(String name) {
        List<FileItem> items = parameters.get(name);
        if (items == null || items.size() == 0) {
            return null;
        } else {
            return items.toArray(new FileItem[items.size()]);
        }
    }

    // --- HttpServletRequestWrapper support ---

    @Override
    public String getParameter(String name) {
        String value = getRequest().getParameter(name);
        if (value != null) {
            return value;
        }

        List<FileItem> items = parameters.get(name);
        if (items == null || items.size() == 0) {
            return null;
        }

        FileItem item = items.get(0);
        if (item.isFormField()) {
            try {
                return item.getString("UTF-8");
            } catch (UnsupportedEncodingException error) {
                throw new IllegalStateException(error);
            }
        }

        return item.getFieldName();
    }

    /** Returns set of all query string and form post parameter names. */
    private Set<String> getNamesSet() {
        @SuppressWarnings("unchecked")
        Enumeration<String> names = getRequest().getParameterNames();
        Set<String> namesSet = new LinkedHashSet<String>();
        while (names.hasMoreElements()) {
            namesSet.add(names.nextElement());
        }
        namesSet.addAll(parameters.keySet());
        return namesSet;
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        Map<String, String[]> map = new LinkedHashMap<String, String[]>();
        for (String name : getNamesSet()) {
            map.put(name, getParameterValues(name));
        }
        return map;
    }

    @Override
    public Enumeration<String> getParameterNames() {
        return Collections.enumeration(getNamesSet());
    }

    @Override
    public String[] getParameterValues(String name) {
        String[] values = getRequest().getParameterValues(name);
        List<FileItem> items = parameters.get(name);

        if (items == null || items.size() == 0) {
            return values;
        }

        List<String> valuesList = new ArrayList<String>();
        if (values != null && values.length > 0) {
            valuesList.addAll(Arrays.asList(values));
        }

        for (FileItem item : items) {
            if (item.isFormField()) {
                try {
                    valuesList.add(item.getString("UTF-8"));
                } catch (UnsupportedEncodingException ex) {
                    valuesList.add(item.getFieldName());
                }
            } else {
                valuesList.add(item.getFieldName());
            }
        }

        return valuesList.toArray(new String[valuesList.size()]);
    }
}
