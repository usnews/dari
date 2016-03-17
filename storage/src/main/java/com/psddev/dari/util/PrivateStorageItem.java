package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;

public class PrivateStorageItem extends AbstractStorageItem {

    private StorageItem item;

    public PrivateStorageItem(StorageItem item) {
        this.item = item;
    }

    @Override
    protected InputStream createData() throws IOException {
        if (item instanceof AbstractStorageItem) {
            return ((AbstractStorageItem) item).createData();
        }

        return null;
    }

    @Override
    protected void saveData(InputStream data) throws IOException {
        if (item instanceof AbstractStorageItem) {
            ((AbstractStorageItem) item).saveData(data);
        }
    }

    @Override
    public boolean isInStorage() {
        return item.isInStorage();
    }

    @Override
    public String getPublicUrl() {
        if (item instanceof StorageItemPrivateUrl) {
            return ((StorageItemPrivateUrl) item).getPrivateUrl();
        }

        return item.getPublicUrl();
    }

}
