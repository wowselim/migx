package co.selim.migx.core.impl.runner;

import co.selim.migx.core.impl.SqlMigrationScript;
import co.selim.migx.core.impl.util.Pair;
import co.selim.migx.core.output.MigrationOutput;
import co.selim.migx.core.output.MigrationOutputBuilder;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.*;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;

import static co.selim.migx.core.impl.util.Checksums.calculateChecksum;
import static co.selim.migx.core.impl.util.Clock.millisSince;
import static co.selim.migx.core.impl.util.Clock.now;

public class MySQLMigrationRunner implements MigrationRunner {

  private static final Tuple LOCK_NAME = Tuple.of("migx");
  private final Vertx vertx;
  private final Pool pool;
  private final AtomicBoolean schemaHistoryCreated = new AtomicBoolean(false);

  public MySQLMigrationRunner(Vertx vertx, Pool pool) {
    this.vertx = vertx;
    this.pool = pool;
  }

  @Override
  public Future<MigrationOutput> run(SqlMigrationScript script) {
    if (schemaHistoryCreated.compareAndSet(false, true)) {
      return createSchemaHistoryTableIfNotExists()
        .compose(x -> doRun(script));
    } else {
      return doRun(script);
    }
  }

  private Future<MigrationOutput> doRun(SqlMigrationScript script) {
    return pool.getConnection()
      .compose(connection -> {
          var output = switch (script.category()) {
            // MySQL has different transaction behavior - DDL causes implicit commit
            case VERSIONED -> lock(connection)
              .compose(x -> runVersionedMigration(connection, script))
              .eventually(() -> unlock(connection));
            // REPEATABLE migrations can run without transaction
            case REPEATABLE -> runRepeatableMigration(connection, script);
          };
          return output.compose(result ->
            result != null ?
              updateHistoryTable(connection, script, result.left(), result.right()) :
              Future.succeededFuture()
          );
        }
      );
  }

  private Future<Void> lock(SqlConnection connection) {
    return connection.preparedQuery("select get_lock(?, 0)")
      .execute(LOCK_NAME)
      .compose(rowSet -> {
        Integer returnValue = rowSet.iterator().next().getInteger(0);
        if (returnValue == null) {
          return Future.failedFuture("Failed to acquire lock");
        }
        if (returnValue == 0) {
          return Future.failedFuture("Timed out while waiting to acquire lock");
        }
        if (returnValue == 1) {
          return Future.succeededFuture();
        }
        return Future.failedFuture(new IllegalStateException("Unexpected result when trying to acquire lock: " + returnValue));
      });
  }

  private Future<Void> unlock(SqlConnection connection) {
    return connection.preparedQuery("select release_lock(?)")
      .execute(LOCK_NAME)
      .compose(rowSet -> {
        Integer returnValue = rowSet.iterator().next().getInteger(0);
        if (returnValue == null) {
          return Future.failedFuture("Expected lock to exist");
        }
        if (returnValue == 0) {
          return Future.failedFuture("Failed to release lock");
        }
        if (returnValue == 1) {
          return Future.succeededFuture();
        }
        return Future.failedFuture(new IllegalStateException("Unexpected result when trying to release lock: " + returnValue));
      });
  }

  private Future<Void> createSchemaHistoryTableIfNotExists() {
    return vertx.fileSystem()
      .readFile("mysql_flyway_schema_history_ddl.sql")
      .compose(buffer -> pool.query(buffer.toString()).execute().mapEmpty());
  }

  private Future<Pair<MigrationOutput, Integer>> runRepeatableMigration(SqlConnection connection, SqlMigrationScript script) {
    return connection.preparedQuery("""
        select checksum FROM flyway_schema_history \
        where script = ? \
        order by installed_rank desc \
        limit 1\
        """)
      .execute(Tuple.of(script.filename()))
      .compose(rowSet -> script.sql()
        .compose(sql -> {
          int currentChecksum = calculateChecksum(sql);
          RowIterator<Row> iterator = rowSet.iterator();

          if (!iterator.hasNext() || iterator.next().getInteger("checksum") != currentChecksum) {
            long startTime = now();
            return connection.query(sql).execute()
              .map(x -> {
                MigrationOutput output = MigrationOutputBuilder.builder()
                  .category(script.category().toString())
                  .version(script.version())
                  .description(script.description())
                  .type("SQL")
                  .filepath(script.filepath())
                  .executionTime(millisSince(startTime))
                  .build();
                return new Pair<>(output, currentChecksum);
              });
          } else {
            return Future.succeededFuture();
          }
        }));
  }

  private Future<Pair<MigrationOutput, Integer>> runVersionedMigration(SqlConnection connection, SqlMigrationScript script) {
    // For MySQL, we need to handle DDL separately since it causes implicit commit
    return connection.preparedQuery("""
        select checksum from flyway_schema_history \
        where version = ?\
        """)
      .execute(Tuple.of(script.version()))
      .compose(rowSet -> script.sql()
        .compose(sql -> {
          int currentChecksum = calculateChecksum(sql);
          RowIterator<Row> iterator = rowSet.iterator();

          if (!iterator.hasNext()) {
            long startTime = now();
            return connection.query(sql).execute()
              .compose(v -> {
                MigrationOutput output = MigrationOutputBuilder.builder()
                  .category(script.category().toString())
                  .version(script.version())
                  .description(script.description())
                  .type("SQL")
                  .filepath(script.filepath())
                  .executionTime(millisSince(startTime))
                  .build();
                return Future.succeededFuture(new Pair<>(output, currentChecksum));
              });
          } else {
            // Existing migration - verify checksum
            int storedChecksum = iterator.next().getInteger("checksum");
            if (storedChecksum != currentChecksum) {
              return Future.failedFuture(new RuntimeException(
                "Checksum mismatch for version " + script.version() +
                  ". Expected " + storedChecksum + " but was " + currentChecksum
              ));
            }
            return Future.succeededFuture(); // Already applied
          }
        }));
  }

  private Future<MigrationOutput> updateHistoryTable(
    SqlConnection connection,
    SqlMigrationScript script,
    MigrationOutput output,
    int checksum
  ) {
    if (script == null) {
      return Future.succeededFuture();
    }

    String sql = """
      insert into flyway_schema_history \
      (installed_rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success) \
      select coalesce(max(installed_rank), 0) + 1, ?, ?, 'SQL', ?, ?, substring_index(current_user(), '@', 1), ?, ?, TRUE \
      from flyway_schema_history\
      """;

    Tuple tuple = Tuple.of(
      script.version().isEmpty() ? null : script.version(),
      script.description(),
      script.filename(),
      checksum,
      LocalDateTime.now(),
      output.executionTime()
    );

    return connection.preparedQuery(sql)
      .execute(tuple)
      .map(output);
  }
}
