package co.selim.migx.core.output;

import java.util.List;

public record MigrationResult(
  String initialSchemaVersion,
  List<MigrationOutput> migrations,
  int migrationsExecuted,
  String schemaName,
  boolean success,
  String targetSchemaVersion
) {
}
