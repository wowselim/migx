package co.selim.migx.core.impl;

import co.selim.migx.core.Migx;
import co.selim.migx.core.impl.util.Paths;
import co.selim.migx.core.output.MigrationOutput;
import co.selim.migx.core.output.MigrationResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.future.CompositeFutureImpl;
import io.vertx.sqlclient.SqlClient;

import java.util.List;
import java.util.Set;

public class SqlClientMigx implements Migx {

  private final Vertx vertx;
  private final SqlClient sqlClient;
  private final String migrationPath;
  private static final Set<String> SUPPORTED_MIGRATION_TYPES = Set.of("Versioned", "Repeatable");

  public SqlClientMigx(Vertx vertx, SqlClient sqlClient) {
    this(vertx, sqlClient, "db/migration");
  }

  public SqlClientMigx(Vertx vertx, SqlClient sqlClient, String migrationPath) {
    this.vertx = vertx;
    this.sqlClient = sqlClient;
    this.migrationPath = migrationPath;
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
      .readDir(migrationPath)
      .compose(migrationScripts -> {
          List<Future<MigrationOutput>> outputs = migrationScripts.stream()
            .filter(path -> SUPPORTED_MIGRATION_TYPES.contains(Paths.getCategoryFromPath(path)))
            .sorted(Paths.MIGRATION_COMPARATOR)
            .map(path ->
              vertx.fileSystem()
                .readFile(path)
                .compose(buffer -> executeMigration(path, buffer.toString()))
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

  private Future<MigrationOutput> executeMigration(String path, String script) {
    long startTime = System.currentTimeMillis();
    return sqlClient.query(script)
      .execute()
      .map(x -> new MigrationOutput(
        Paths.getCategoryFromPath(path),
        Paths.getDescriptionFromPath(path),
        (int) (System.currentTimeMillis() - startTime),
        path,
        "SQL",
        Paths.getVersionFromPath(path)));
  }

  private <T> CompositeFuture all(List<Future<T>> futures) {
    return CompositeFutureImpl.all(futures.toArray(Future[]::new));
  }
}
