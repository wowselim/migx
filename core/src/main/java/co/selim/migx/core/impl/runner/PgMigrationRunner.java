package co.selim.migx.core.impl.runner;

import co.selim.migx.core.impl.SqlMigrationScript;
import co.selim.migx.core.impl.util.Checksums;
import co.selim.migx.core.impl.util.Clock;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
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
      .compose(x -> switch (script.category()) {
        case VERSIONED -> runVersionedMigration(script);
        case REPEATABLE -> runRepeatableMigration(script);
      })
      .compose(output -> unlock().map(output))
      .recover(t ->
        unlock()
          .compose(x -> Future.failedFuture(t))
      );
  }

  private Future<MigrationOutput> runRepeatableMigration(SqlMigrationScript script) {
    return sqlClient.preparedQuery("""
        select checksum from flyway_schema_history \
        where script = $1 \
        order by installed_rank desc \
        limit 1\
        """)
      .execute(Tuple.of(script.version()))
      .compose(rowSet -> script.sql()
        .compose(sql -> {
          int currentChecksum = Checksums.calculateChecksum(sql);
          RowIterator<Row> iterator = rowSet.iterator();

          if (!iterator.hasNext() || iterator.next().getInteger("checksum") != currentChecksum) {
            long startTime = Clock.now();
            return sqlClient.query(sql).execute()
              .map(x -> new MigrationOutput(
                script,
                Clock.millisSince(startTime),
                currentChecksum
              ));
          } else {
            return Future.succeededFuture();
          }
        }));
  }

  private Future<MigrationOutput> runVersionedMigration(SqlMigrationScript script) {
    return sqlClient.preparedQuery("""
        select checksum from flyway_schema_history \
        where version = $1\
        """)
      .execute(Tuple.of(script.filename()))
      .compose(rowSet -> script.sql()
        .compose(sql -> {
          int currentChecksum = Checksums.calculateChecksum(sql);
          RowIterator<Row> iterator = rowSet.iterator();

          if (!iterator.hasNext()) {
            long startTime = Clock.now();
            return sqlClient.query(sql).execute().map(sql)
              .map(x -> new MigrationOutput(
                script,
                Clock.millisSince(startTime),
                Checksums.calculateChecksum(sql)
              ));
          } else {
            // Existing migration - verify checksum
            int storedChecksum = iterator.next().getInteger("checksum");
            if (storedChecksum != currentChecksum) {
              return Future.failedFuture(new RuntimeException(
                "Checksum mismatch for version " + script.version() +
                  ". Expected " + storedChecksum + " but was " + currentChecksum
              ));
            }
            return Future.succeededFuture(null); // Already applied
          }
        }));
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
