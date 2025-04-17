package co.selim.migx.core.impl;

public record SqlMigrationScript(
  String path,
  String filename,
  String sql,
  String description,
  int checksum,
  Category category,
  String version
) implements Comparable<SqlMigrationScript> {

  enum Category {
    BASELINE('B', 0), VERSIONED('V', 1), REPEATABLE('R', 2);

    private final char identifier;
    private final int order;

    Category(char identifier, int order) {
      this.identifier = identifier;
      this.order = order;
    }

    static Category fromChar(char identifier) {
      return switch (Character.toUpperCase(identifier)) {
        case 'B' -> BASELINE;
        case 'V' -> VERSIONED;
        case 'R' -> REPEATABLE;
        case 'U' -> throw new IllegalArgumentException("Undo migrations are not supported");
        default -> throw new IllegalArgumentException("Unknown migration script type: " + identifier);
      };
    }

    @Override
    public String toString() {
      return switch (this) {
        case BASELINE -> "Baseline";
        case VERSIONED -> "Versioned";
        case REPEATABLE -> "Repeatable";
      };
    }
  }

  @Override
  public int compareTo(SqlMigrationScript o) {
    if (category != o.category) {
      return Integer.compare(category.order, o.category.order);
    }
    return description.compareTo(o.description);
  }
}
