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

public class PgMigrationRunner implements MigrationRunner {

  private static final Tuple LOCK_ID = Tuple.of("migx".hashCode());
  private final Vertx vertx;
  private final Pool pool;
  private final AtomicBoolean schemaHistoryCreated = new AtomicBoolean(false);

  public PgMigrationRunner(Vertx vertx, Pool pool) {
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
    return pool.withTransaction(connection ->
      lock(connection)
        .compose(x -> switch (script.category()) {
          case VERSIONED -> runVersionedMigration(connection, script);
          case REPEATABLE -> runRepeatableMigration(connection, script);
        })
        .compose(result ->
          result == null ?
            Future.succeededFuture() :
            updateHistoryTable(connection, script, result.left(), result.right())
        )
    );
  }

  private Future<Void> createSchemaHistoryTableIfNotExists() {
    return vertx.fileSystem()
      .readFile("pg_flyway_schema_history_ddl.sql")
      .compose(buffer -> pool.query(buffer.toString()).execute().mapEmpty());
  }

  private Future<Pair<MigrationOutput, Integer>> runRepeatableMigration(SqlConnection connection, SqlMigrationScript script) {
    return connection.preparedQuery("""
        select checksum from flyway_schema_history \
        where script = $1 \
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
    return connection.preparedQuery("""
        select checksum from flyway_schema_history \
        where version = $1\
        """)
      .execute(Tuple.of(script.version()))
      .compose(rowSet -> script.sql()
        .compose(sql -> {
          int currentChecksum = calculateChecksum(sql);
          RowIterator<Row> iterator = rowSet.iterator();

          if (!iterator.hasNext()) {
            long startTime = now();
            return connection.query(sql).execute().map(sql)
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

  private Future<Void> lock(SqlConnection connection) {
    return connection.preparedQuery("select pg_advisory_xact_lock($1)")
      .execute(LOCK_ID)
      .mapEmpty();
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
      select coalesce(max(installed_rank), 0) + 1, $1, $2, 'SQL', $3, $4, current_user, $5, $6, TRUE \
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
