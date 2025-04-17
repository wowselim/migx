package co.selim.migx.core.impl.util;

public final class Paths {

  private static final String VERSION_SEPARATOR = "__";

  private Paths() {
  }

  public static String getDescriptionFromFilename(String filename) {
    filename = filename.substring(filename.indexOf(VERSION_SEPARATOR) + VERSION_SEPARATOR.length());
    filename = filename.substring(0, filename.lastIndexOf("."));
    return filename.replace('_', ' ');
  }

  public static char getCategoryFromFilename(String filename) {
    return filename.charAt(0);
  }

  public static String getVersionFromFilename(String filename) {
    return filename.substring(1, filename.indexOf(VERSION_SEPARATOR));
  }
}
