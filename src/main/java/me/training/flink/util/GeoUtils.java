package me.training.flink.util;

public class GeoUtils {
    // geo boundaries of the area of NYC
    public static double LON_EAST = -73.7;
    public static double LON_WEST = -74.05;
    public static double LAT_NORTH = 41.0;
    public static double LAT_SOUTH = 40.5;

    // area width and height
    public static double LON_WIDTH = LON_EAST - LON_WEST;
    public static double LAT_HEIGHT = LAT_NORTH - LAT_SOUTH;

    // delta step to create artificial grid overlay of NYC
    public static double DELTA_LON = 0.0014;
    public static double DELTA_LAT = 0.00125;

    // ( |LON_WEST| - |LON_EAST| ) / DELTA_LAT
    public static int NUMBER_OF_GRID_X = 250;
    // ( LAT_NORTH - LAT_SOUTH ) / DELTA_LON
    public static int NUMBER_OF_GRID_Y = 400;

    public static float DEG_LEN = 110.25f;

    public static boolean isInNYC(float lon, float lat) {

        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

    public static int mapToGridCell(float lon, float lat) {
        int xIndex = (int)Math.floor((Math.abs(LON_WEST) - Math.abs(lon)) / DELTA_LON);
        int yIndex = (int)Math.floor((LAT_NORTH - lat) / DELTA_LAT);

        return xIndex + (yIndex * NUMBER_OF_GRID_X);
    }
}
