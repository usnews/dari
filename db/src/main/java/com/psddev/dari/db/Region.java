package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Region {

    public static final int EARTH_RADIUS_IN_MILES = 3959;
    public static final int EARTH_RADIUS_IN_KILOMETERS = 6371;

    private List<List<Location>> shapes;
    private Double x;
    private Double y;
    private Double radius;

    public static double milesToDegrees(double miles) {
        return Math.toDegrees(miles / EARTH_RADIUS_IN_MILES);
    }

    public static double kilometersToDegrees(double kilometers) {
        return Math.toDegrees(kilometers / EARTH_RADIUS_IN_KILOMETERS);
    }

    public static double degreesToMiles(double degrees) {
        return EARTH_RADIUS_IN_MILES * Math.toRadians(degrees);
    }

    public static double degreesToKilometers(double degrees) {
        return EARTH_RADIUS_IN_KILOMETERS * Math.toRadians(degrees);
    }

    /** Creates an empty region. */
    public static Region empty() {
        return new Region();
    }

    /** Creates a circular region on a Cartesian coordinate system. */
    public static Region cartesianCircle(double x, double y, double radius, int sides) {
        return new Region(x, y, radius, sides);
    }

    /**
     * Creates a circular region on a spherical coordinate system, where x is
     * latitude and y is longitude.
     *
     * All arguments should be given as degrees.
     */
    public static Region sphericalCircle(double x, double y, double radius) {
        return new Region(x, y, radius, locationsLatLon(x, y, radius));
    }

    private Region(Double x, Double y, Double radius, List<Location> locations) {
        this.x = x;
        this.y = y;
        this.radius = radius;
        this.shapes = new ArrayList<List<Location>>(); 

        shapes.add(locations);
    }

    /**
     * Creates an empty region.
     * @deprecated Use {@link #empty()} instead.
     */
    @Deprecated
    public Region() {
        this.shapes = new ArrayList<List<Location>>(); 
    }

    /**
     * Creates a circular region on a Cartesian coordinate system.
     * @deprecated Use {@link #cartesianCircle(double, double, double, int)} instead.
     */
    @Deprecated
    public Region(double x, double y, double radius, int sides, double initialAngle) {
        // initialAngle has always been ignored. Keeping it here for API compatibility.
        this(x, y, radius, sides);
    }

    /**
     * Creates a circular region on a Cartesian coordinate system.
     * @deprecated Use {@link #cartesianCircle(double, double, double, int)} instead.
     */
    @Deprecated
    public Region(double x, double y, double radius, int sides) {
        this(x, y, radius, locationsFlat(x, y, radius, sides));
    }

    // Generates a polygon of locations on a Cartesian coordinate system.
    private static List<Location> locationsFlat(double x, double y, double radius, int sides) {
        List<Location> locations = new ArrayList<Location>();
        for (double inc = 360.0 / sides, i = 360.0; i >= 0.0; i -= inc) {
            double a = Math.toRadians(i - 270.0);
            locations.add(new Location(x + radius * Math.cos(a), y + radius * Math.sin(a)));
        }
        return Collections.unmodifiableList(locations);
    }

    // Generates rectangular boundary points on a spherical surface.
    private static List<Location> locationsLatLon(double x, double y, double radius) {

        List<Location> locations = new ArrayList<Location>();
        double deltaY = radius * Math.cos(Math.toRadians(x));

        locations.add(new Location(x + radius, y));
        locations.add(new Location(x, y + deltaY));
        locations.add(new Location(x - radius, y));
        locations.add(new Location(x, y - deltaY));
        locations.add(new Location(x + radius, y));

        return Collections.unmodifiableList(locations);
    }

    /** Returns an unmodifiable list of locations. */
    public List<Location> getLocations() {
        if (shapes.size() > 0) {
            return Collections.unmodifiableList(shapes.get(0));
        }

        return null;
    }

    public Double getX() {
        return x;
    }

    public Double getY() {
        return y;
    }

    public Double getRadius() {
        return radius;
    }

    public void addShape(List<Location> shape) {
        shapes.add(shape);
    }

    public List<List<Location>> getShapes() {
        return shapes;
    }

    /** Returns {@code true} if this region is a circle. */
    public boolean isCircle() {
        return getRadius() != null;
    }
}
