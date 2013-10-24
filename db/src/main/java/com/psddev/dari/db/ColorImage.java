package com.psddev.dari.db;

import java.io.IOException;

import com.psddev.dari.util.StorageItem;

/**
 * Implement this interface to automatically analyze an image for
 * {@link ColorDistribution} information and enable search by color.
 */
public interface ColorImage extends Recordable {

    /**
     * Returns the image that should be analyzed for {@link ColorDistribution}
     * information.
     *
     * @return May be {@code null}.
     */
    public StorageItem getColorImage();
}

class ColorImageData extends Modification<ColorImage> {

    @Override
    protected void beforeSave() {
        ColorDistribution.Data distributionData = as(ColorDistribution.Data.class);

        if (distributionData.getDistribution() == null) {
            try {
                distributionData.setDistribution(ColorDistribution.Static.createDistribution(getOriginalObject().getColorImage()));

            } catch (IOException error) {
                throw new IllegalArgumentException(error);
            }
        }
    }
}
