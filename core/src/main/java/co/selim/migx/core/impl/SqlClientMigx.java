package co.selim.migx.core.impl;

import co.selim.migx.core.Migx;
import co.selim.migx.core.output.MigrationOutput;
import co.selim.migx.core.output.MigrationResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.future.CompositeFutureImpl;
import io.vertx.sqlclient.SqlClient;

import java.util.List;

public class SqlClientMigx implements Migx {

  private final Vertx vertx;
  private final SqlClient sqlClient;

  public SqlClientMigx(Vertx vertx, SqlClient sqlClient) {
    this.vertx = vertx;
    this.sqlClient = sqlClient;
  }

  @Override
  public Future<MigrationResult> migrate() {
    return createSchemaHistoryTableIfNotExists()
      .compose(empty -> runMigrations());
  }

  private Future<Void> createSchemaHistoryTableIfNotExists() {
    return vertx.fileSystem()
      .readFile("flyway_schema_history_ddl.sql")
      .compose(buffer -> sqlClient.query(buffer.toString()).execute().mapEmpty());
  }

  private Future<MigrationResult> runMigrations() {
    return vertx.fileSystem()
      .readDir("db/migration")
      .compose(migrationScripts -> {
          List<Future<MigrationOutput>> outputs = migrationScripts.stream()
            .sorted()
            .map(fileName ->
              vertx.fileSystem()
                .readFile(fileName)
                .compose(buffer -> executeMigration(buffer.toString()))
            )
            .toList();
          return all(outputs);
        }
      )
      .map(x -> {
        List<MigrationOutput> migrationOutputs = x.list();
        return new MigrationResult(
          "TODO",
          migrationOutputs,
          migrationOutputs.size(),
          "TODO",
          true,
          "TODO"
        );
      });
  }

  private Future<MigrationOutput> executeMigration(String script) {
    return sqlClient.query(script)
      .execute()
      .map(x -> new MigrationOutput("TODO",
        "TODO",
        0,
        "TODO",
        "TODO",
        "TODO"));
  }

  private <T> CompositeFuture all(List<Future<T>> futures) {
    return CompositeFutureImpl.all(futures.toArray(Future[]::new));
  }
}
