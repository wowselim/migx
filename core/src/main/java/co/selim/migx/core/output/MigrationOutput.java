package co.selim.migx.core.output;

public record MigrationOutput(
  String category,
  String description,
  int executionTime,
  String filepath,
  String type,
  String version
) {
}
