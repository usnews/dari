package com.psddev.dari.util;

import java.io.IOException;

public abstract class StorageItemPlugin {

    protected transient StorageItem item;

    public StorageItemPlugin(StorageItem item) {
        this.item = item;
    }

    public abstract void process() throws IOException;

}
