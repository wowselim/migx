package co.selim.migx.core.impl.runner;

import co.selim.migx.core.impl.SqlMigrationScript;
import co.selim.migx.core.impl.util.Checksums;
import co.selim.migx.core.impl.util.Clock;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;

import java.time.LocalDateTime;

public class PgMigrationRunner {

  private static final Tuple LOCK_ID = Tuple.of("migx".hashCode());
  private final Vertx vertx;
  private final SqlClient sqlClient;

  public PgMigrationRunner(Vertx vertx, SqlClient sqlClient) {
    this.vertx = vertx;
    this.sqlClient = sqlClient;
  }

  public Future<MigrationOutput> run(SqlMigrationScript script) {
    return lock()
      .compose(x -> sqlClient.preparedQuery("SELECT version FROM flyway_schema_history WHERE version = $1")
        .execute(Tuple.of(script.version())))
      .compose(rowSet -> {
        if (!rowSet.iterator().hasNext()) {
          long startTime = Clock.now();
          return script.sql()
            .compose(sql -> sqlClient.query(sql).execute().map(sql))
            .map(sql -> new MigrationOutput(script, Clock.millisSince(startTime), Checksums.calculateChecksum(sql)));
        } else {
          return Future.succeededFuture(); // already applied
        }
      })
      .compose(output -> unlock().map(output))
      .recover(t ->
        unlock()
          .compose(x -> Future.failedFuture(t))
      );
  }

  public Future<Void> createSchemaHistoryTableIfNotExists() {
    return vertx.fileSystem()
      .readFile("pg_flyway_schema_history_ddl.sql")
      .compose(buffer -> sqlClient.query(buffer.toString()).execute().mapEmpty());
  }

  private Future<Void> lock() {
    return sqlClient.preparedQuery("select pg_advisory_lock($1)")
      .execute(LOCK_ID)
      .mapEmpty();
  }

  private Future<Void> unlock() {
    return sqlClient.preparedQuery("select pg_advisory_unlock($1)")
      .execute(LOCK_ID)
      .mapEmpty();
  }

  public Future<MigrationOutput> updateHistoryTable(MigrationOutput migrationOutput) {
    if (migrationOutput == null) {
      return Future.succeededFuture();
    }

    String sql = """
      insert into flyway_schema_history \
      select coalesce(max(installed_rank), 0) + 1, $1, $2, 'SQL', $3, $4, current_user, $5, $6, $7 \
      from flyway_schema_history\
      """;

    SqlMigrationScript script = migrationOutput.script();
    Tuple tuple = Tuple.of(
      script.version(),
      script.description(),
      script.filename(),
      migrationOutput.checksum(),
      LocalDateTime.now(),
      migrationOutput.executionTime(),
      true
    );

    return sqlClient.preparedQuery(sql)
      .execute(tuple)
      .map(migrationOutput);
  }
}
