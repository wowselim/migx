package co.selim.migx.core.output;

import co.selim.migx.core.impl.SqlMigrationScript;

public record MigrationOutput(
  SqlMigrationScript script,
  long executionTime,
  int checksum
) {
}
