
package com.psddev.dari.util;

import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaImageCacheManagementTask extends Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(JavaImageCacheManagementTask.class);
    public static final long MEGA_BYTE = 1048576;

    private Long maximumCacheSize;
    private String baseFolder;

    public JavaImageCacheManagementTask(String baseFolder, Long maximumCacheSize) {
        this.baseFolder = baseFolder;

        if (maximumCacheSize == null) {
            this.maximumCacheSize = 500 * MEGA_BYTE;
        } else {
            this.maximumCacheSize = maximumCacheSize;
        }
    }

    public Long getMaximumCacheSize() {
        return maximumCacheSize;
    }

    public void setMaximumCacheSize(Long maximumCacheSize) {
        this.maximumCacheSize = maximumCacheSize;
    }

    public String getBaseFolder() {
        return baseFolder;
    }

    public void setBaseFolder(String baseFolder) {
        this.baseFolder = baseFolder;
    }

    @Override
    protected void doTask() throws Exception {
        JavaImageEditor javaImageEditor = ObjectUtils.to(JavaImageEditor.class, ImageEditor.Static.getInstance(ImageEditor.JAVA_IMAGE_EDITOR_NAME));
        String cachePath = javaImageEditor.getCachePath();

        File imageFolder = new File(cachePath);
        if (imageFolder.exists()) {
            Long cacheSize = FileUtils.sizeOfDirectory(imageFolder);

            if (cacheSize > maximumCacheSize) {
                LOGGER.info(String.format("Image cache size (%.2fmb) exceeds limit of %.2fmb. Attempting to resize to 80%% of limit.", ((float) cacheSize / MEGA_BYTE), ((float) maximumCacheSize / MEGA_BYTE)));

                //Re-size the cache to 80% that of CACHE_LIMIT
                long minimumBytesToDelete = cacheSize - maximumCacheSize + (new Double(maximumCacheSize * 0.2)).longValue();
                long bytesDeleted = 0;

                //Sorting files by last accessed, last modified if last accessed is not available
                TreeMap<String, File> fileMap = new TreeMap<String, File>();

                Iterator iterator =  FileUtils.iterateFiles(imageFolder, null, true);

                FileSystem fileSystem = FileSystems.getDefault();
                while (iterator.hasNext()) {
                    File file = (File) iterator.next();
                    if (file.isFile() && !file.isHidden()) {
                        Path path = fileSystem.getPath(file.getAbsolutePath());
                        BasicFileAttributes basicFileAttributes = Files.readAttributes(path, BasicFileAttributes.class);
                        if (basicFileAttributes.lastAccessTime() != null) {
                            fileMap.put(basicFileAttributes.lastAccessTime().toMillis() + file.getAbsolutePath(), file);
                        } else {
                            fileMap.put(file.lastModified() + file.getAbsolutePath(), file);
                        }
                    }
                }

                int filesDeleted = 0;
                for (File file : fileMap.values()) {
                    long fileSize = file.length(); //Get before deleting
                    if (file.delete()) {
                        filesDeleted++;
                        bytesDeleted += fileSize;
                        if (bytesDeleted >= minimumBytesToDelete) {
                            break;
                        }
                    }
                }

                if (bytesDeleted < minimumBytesToDelete) {
                    LOGGER.error(String.format("Unable to resize image cache to less than %.2fmb. Current size %.2fmb", ((float) (maximumCacheSize * .8 / MEGA_BYTE)), ((float) (cacheSize - bytesDeleted) / MEGA_BYTE)));
                } else {
                    LOGGER.info(String.format("Removed %s files, recoverd %.2fmb. Current size %.2fmb", filesDeleted, ((float) bytesDeleted / MEGA_BYTE), ((float) (cacheSize - bytesDeleted) / MEGA_BYTE)));
                }

            }
        }
    }
}
