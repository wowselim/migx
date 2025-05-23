package co.selim.migx.core.impl.util;

import java.io.File;

public final class Paths {

  private static final String VERSION_SEPARATOR = "__";

  private Paths() {
  }

  public static String getDescriptionFromFilename(String filename) {
    return filename.substring(0, filename.lastIndexOf("."))
      .substring(filename.indexOf(VERSION_SEPARATOR) + VERSION_SEPARATOR.length())
      .replace('_', ' ');
  }

  public static char getCategoryFromFilename(String filename) {
    return filename.charAt(0);
  }

  public static String getVersionFromFilename(String filename) {
    if (filename.startsWith("R")) return "";
    return filename.substring(1, filename.indexOf(VERSION_SEPARATOR));
  }

  public static String getFilename(String path) {
    return path.substring(path.lastIndexOf(File.separatorChar) + 1);
  }
}
