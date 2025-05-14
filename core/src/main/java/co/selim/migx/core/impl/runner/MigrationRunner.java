package co.selim.migx.core.impl.runner;

import co.selim.migx.core.impl.SqlMigrationScript;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Future;

public interface MigrationRunner {

  Future<MigrationOutput> run(SqlMigrationScript script);
}
