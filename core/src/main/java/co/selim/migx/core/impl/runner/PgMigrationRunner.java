package co.selim.migx.core.impl.runner;

import co.selim.migx.core.impl.SqlMigrationScript;
import co.selim.migx.core.impl.util.Checksums;
import co.selim.migx.core.impl.util.Clock;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.*;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;

public class PgMigrationRunner {

  private static final Tuple LOCK_ID = Tuple.of("migx".hashCode());
  private final Vertx vertx;
  private final Pool pool;
  private final AtomicBoolean schemaHistoryCreated = new AtomicBoolean(false);

  public PgMigrationRunner(Vertx vertx, Pool pool) {
    this.vertx = vertx;
    this.pool = pool;
  }

  public Future<MigrationOutput> run(SqlMigrationScript script) {
    Future<Void> start;
    if (schemaHistoryCreated.compareAndSet(false, true)) {
      start = createSchemaHistoryTableIfNotExists();
    } else {
      start = Future.succeededFuture();
    }

    return start
      .compose(v ->
        pool.withTransaction(c ->
          lock(c)
            .compose(x -> switch (script.category()) {
              case VERSIONED -> runVersionedMigration(c, script);
              case REPEATABLE -> runRepeatableMigration(c, script);
            })
            .compose(output -> updateHistoryTable(c, output))
        ));
  }

  private Future<Void> createSchemaHistoryTableIfNotExists() {
    return vertx.fileSystem()
      .readFile("pg_flyway_schema_history_ddl.sql")
      .compose(buffer -> pool.query(buffer.toString()).execute().mapEmpty());
  }

  private Future<MigrationOutput> runRepeatableMigration(SqlConnection connection, SqlMigrationScript script) {
    return connection.preparedQuery("""
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
            return connection.query(sql).execute()
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

  private Future<MigrationOutput> runVersionedMigration(SqlConnection connection, SqlMigrationScript script) {
    return connection.preparedQuery("""
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
            return connection.query(sql).execute().map(sql)
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

  private Future<Void> lock(SqlConnection connection) {
    return connection.preparedQuery("select pg_advisory_xact_lock($1)")
      .execute(LOCK_ID)
      .mapEmpty();
  }

  private Future<MigrationOutput> updateHistoryTable(SqlConnection connection, MigrationOutput migrationOutput) {
    if (migrationOutput == null) {
      return Future.succeededFuture();
    }

    String sql = """
      insert into flyway_schema_history \
      (installed_rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success) \
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

    return connection.preparedQuery(sql)
      .execute(tuple)
      .map(migrationOutput);
  }
}
