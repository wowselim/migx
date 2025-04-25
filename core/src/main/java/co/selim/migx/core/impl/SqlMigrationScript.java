package co.selim.migx.core.impl;

import io.vertx.core.Future;

public record SqlMigrationScript(
  String path,
  String filename,
  Future<String> sql,
  String description,
  Category category,
  String version
) {

  public enum Category {
    BASELINE, VERSIONED, REPEATABLE;

    public static Category fromChar(char identifier) {
      return switch (identifier) {
        case 'B' -> BASELINE;
        case 'V' -> VERSIONED;
        case 'R' -> REPEATABLE;
        case 'U' -> throw new IllegalArgumentException("Undo migrations are not supported");
        default -> throw new IllegalArgumentException("Unknown migration type: " + identifier);
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
}
