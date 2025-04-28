package co.selim.migx.core.impl;

import co.selim.migx.core.Migx;
import co.selim.migx.core.impl.runner.PgMigrationRunner;
import co.selim.migx.core.impl.util.MigrationComparator;
import co.selim.migx.core.impl.util.Paths;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.Pool;

import java.util.List;

public class PoolMigx implements Migx {

  private final Vertx vertx;
  private final List<String> migrationPaths;
  private final PgMigrationRunner pgMigrationRunner;

  public PoolMigx(Vertx vertx, Pool pool) {
    this(vertx, pool, List.of("db/migration"));
  }

  public PoolMigx(Vertx vertx, Pool pool, List<String> migrationPaths) {
    this.vertx = vertx;
    this.migrationPaths = migrationPaths;
    this.pgMigrationRunner = new PgMigrationRunner(vertx, pool);
  }

  @Override
  public Future<Void> migrate() {
    return runMigrations().mapEmpty();
  }

  private Future<MigrationOutput> runMigrations() {
    List<Future<List<String>>> migrationFiles = migrationPaths.stream()
      .map(path -> vertx.fileSystem().readDir(path))
      .toList();

    return Future.all(migrationFiles)
      .compose(files -> {
        List<List<String>> migrationScripts = files.list();
        List<String> allMigrations = migrationScripts.stream()
          .flatMap(List::stream)
          .sorted(new MigrationComparator())
          .toList();

        return executeMigrationsSerially(allMigrations);
      });
  }

  private Future<MigrationOutput> executeMigrationsSerially(List<String> paths) {
    Future<MigrationOutput> chain = Future.succeededFuture();
    for (String path : paths) {
      chain = chain.compose(x ->
        loadMigrationScript(path)
          .compose(pgMigrationRunner::run)
      );
    }
    return chain;
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
}
