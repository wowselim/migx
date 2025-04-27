package co.selim.migx.core.impl.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MigrationComparator implements Comparator<String> {

  private static final String VERSION_SEPARATOR = "__";

  @Override
  public int compare(String path1, String path2) {
    String filename1 = path1.substring(path1.lastIndexOf(File.separatorChar) + 1);
    String filename2 = path2.substring(path2.lastIndexOf(File.separatorChar) + 1);

    int categoryOrder1 = getTypeOrder(filename1.charAt(0));
    int categoryOrder2 = getTypeOrder(filename2.charAt(0));

    int typeComparison = Integer.compare(categoryOrder1, categoryOrder2);
    if (typeComparison != 0) return typeComparison;

    List<Integer> version1 = extractVersion(filename1, categoryOrder1);
    List<Integer> version2 = extractVersion(filename2, categoryOrder2);

    if (version1 == null && version2 != null) return 1;
    if (version1 != null && version2 == null) return -1;
    if (version1 != null) {
      return compareVersions(version1, version2);
    }

    return 0;
  }

  private int getTypeOrder(char type) {
    return switch (type) {
      case 'B' -> 0;
      case 'V' -> 1;
      case 'R' -> 2;
      default -> throw new IllegalArgumentException("Unsupported migration type: " + type);
    };
  }

  private List<Integer> extractVersion(String filename, int categoryOrder) {
    if (categoryOrder == 0 || categoryOrder == 1) {
      int versionEnd = filename.indexOf(VERSION_SEPARATOR);
      if (versionEnd > 1) {
        String versionPart = filename.substring(1, versionEnd);
        return parseVersion(versionPart);
      }
    }
    return null; // No version for Repeatable migrations or invalid format
  }

  private List<Integer> parseVersion(String versionStr) {
    List<Integer> version = new ArrayList<>(versionStr.length() / 2 + 1);
    int current = 0;
    boolean hasDigit = false;

    for (int i = 0; i < versionStr.length(); i++) {
      char c = versionStr.charAt(i);
      if (c == '.' || c == '_') {
        version.add(hasDigit ? current : 0);
        current = 0;
        hasDigit = false;
      } else if (Character.isDigit(c)) {
        current = current * 10 + (c - '0');
        hasDigit = true;
      } else {
        version.add(0);
        current = 0;
        hasDigit = false;
      }
    }

    if (hasDigit) {
      version.add(current);
    }

    return version;
  }

  private int compareVersions(List<Integer> v1, List<Integer> v2) {
    int len = Math.max(v1.size(), v2.size());
    for (int i = 0; i < len; i++) {
      int num1 = i < v1.size() ? v1.get(i) : 0;
      int num2 = i < v2.size() ? v2.get(i) : 0;
      int cmp = Integer.compare(num1, num2);
      if (cmp != 0) return cmp;
    }
    return 0;
  }
}
