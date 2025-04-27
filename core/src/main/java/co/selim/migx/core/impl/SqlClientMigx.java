package co.selim.migx.core.impl;

import co.selim.migx.core.Migx;
import co.selim.migx.core.impl.runner.PgMigrationRunner;
import co.selim.migx.core.impl.util.MigrationComparator;
import co.selim.migx.core.impl.util.Paths;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.SqlClient;

import java.util.List;

public class SqlClientMigx implements Migx {

  private final Vertx vertx;
  private final List<String> migrationPaths;
  private final PgMigrationRunner pgMigrationRunner;

  public SqlClientMigx(Vertx vertx, SqlClient sqlClient) {
    this(vertx, sqlClient, List.of("db/migration"));
  }

  public SqlClientMigx(Vertx vertx, SqlClient sqlClient, List<String> migrationPaths) {
    this.vertx = vertx;
    this.migrationPaths = migrationPaths;
    this.pgMigrationRunner = new PgMigrationRunner(vertx, sqlClient);
  }

  @Override
  public Future<Void> migrate() {
    return pgMigrationRunner.createSchemaHistoryTableIfNotExists()
      .compose(empty -> runMigrations().mapEmpty());
  }

  private Future<MigrationOutput> runMigrations() {
    List<Future<List<String>>> migrationFiles = migrationPaths.stream()
      .map(path -> vertx.fileSystem().readDir(path))
      .toList();

    return Future.all(migrationFiles)
      .compose(files -> {
        List<List<String>> migrationScripts = files.list();
        return migrationScripts.stream()
          .flatMap(List::stream)
          .sorted(new MigrationComparator())
          .map(path ->
            loadMigrationScript(path)
              .compose(this::executeMigration)
              .compose(this::updateHistoryTable)
          )
          .reduce((a, b) -> a.compose(x -> b))
          .orElse(Future.succeededFuture());
      });
  }

  private Future<SqlMigrationScript> loadMigrationScript(String path) {
    Future<String> fileReader = vertx.fileSystem()
      .readFile(path)
      .map(Buffer::toString);

    String filename = Paths.getFilename(path);
    return Future.succeededFuture(new SqlMigrationScript(
        path,
        filename,
        fileReader,
        Paths.getDescriptionFromFilename(filename),
        SqlMigrationScript.Category.fromChar(Paths.getCategoryFromFilename(filename)),
        Paths.getVersionFromFilename(filename)
      )
    );
  }

  private Future<MigrationOutput> executeMigration(SqlMigrationScript script) {
    return pgMigrationRunner.run(script);
  }

  private Future<MigrationOutput> updateHistoryTable(MigrationOutput migrationOutput) {
    return pgMigrationRunner.updateHistoryTable(migrationOutput);
  }
}
