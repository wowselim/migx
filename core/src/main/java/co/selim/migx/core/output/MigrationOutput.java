package co.selim.migx.core.output;

import io.avaje.recordbuilder.RecordBuilder;

@RecordBuilder
public record MigrationOutput(
  String category,
  String version,
  String description,
  String type,
  String filepath,
  long executionTime
) {
}
