package com.psddev.dari.util;

import java.io.IOException;

public interface StorageItemPlugin {

    public void process(StorageItem item) throws IOException;

}
