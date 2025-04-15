package co.selim.migx.core.impl.util;

import java.io.File;
import java.util.Comparator;

public final class Paths {

  private static final String VERSION_SEPARATOR = "__";

  private Paths() {
  }

  public static final Comparator<String> MIGRATION_COMPARATOR = (pathA, pathB) -> {
    String fileA = new File(pathA).getName();
    String fileB = new File(pathB).getName();

    char categoryA = Character.toUpperCase(fileA.charAt(0));
    char categoryB = Character.toUpperCase(fileB.charAt(0));

    int orderA = getOrderByCategory(categoryA);
    int orderB = getOrderByCategory(categoryB);

    if (orderA != orderB) {
      return Integer.compare(orderA, orderB);
    }

    String keyA = fileA.substring(1);
    String keyB = fileB.substring(1);
    return keyA.compareToIgnoreCase(keyB);
  };

  private static int getOrderByCategory(char type) {
    return switch (type) {
      case 'B' -> 0;
      case 'V' -> 1;
      case 'R' -> 2;
      default -> 3;
    };
  }

  public static String getCategoryFromPath(String path) {
    String filename = new File(path).getName();
    char firstChar = filename.charAt(0);
    return switch (firstChar) {
      case 'R' -> "Repeatable";
      case 'V' -> "Versioned";
      case 'B' -> "Baseline";
      case 'U' -> throw new UnsupportedOperationException("Undo migrations are not supported");
      default -> throw new IllegalArgumentException("Unknown migration category " + firstChar);
    };
  }

  public static String getDescriptionFromPath(String path) {
    String filename = new File(path).getName();
    filename = filename.substring(filename.indexOf(VERSION_SEPARATOR) + VERSION_SEPARATOR.length());
    filename = filename.substring(0, filename.lastIndexOf("."));
    return filename.replace('_', ' ');
  }

  public static String getVersionFromPath(String path) {
    String filename = new File(path).getName();
    return filename.substring(1, filename.indexOf(VERSION_SEPARATOR));
  }
}
