package com.psddev.dari.util;

import java.io.IOException;

/**
 * StorageItemBeforeCreate allows an Application to implement custom
 * validation of a StorageItemPart prior to creating StorageItems. The validation
 * is performed by {@link StorageItemFilter}.
 */
public interface StorageItemBeforeCreate {

    /**
     * Invoked by {@link StorageItemFilter}
     *
     * @param part StorageItemPart containing relevant data
     * @throws IOException
     */
    void beforeCreate(StorageItemUploadPart part) throws IOException;
}