package co.selim.migx.core.impl;

import co.selim.migx.core.Migx;
import co.selim.migx.core.impl.util.Checksums;
import co.selim.migx.core.impl.util.Paths;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;

public class SqlClientMigx implements Migx {

  private final Vertx vertx;
  private final SqlClient sqlClient;
  private final List<String> migrationPaths;

  public SqlClientMigx(Vertx vertx, SqlClient sqlClient) {
    this(vertx, sqlClient, List.of("db/migration"));
  }

  public SqlClientMigx(Vertx vertx, SqlClient sqlClient, List<String> migrationPaths) {
    this.vertx = vertx;
    this.sqlClient = sqlClient;
    this.migrationPaths = migrationPaths;
  }

  @Override
  public Future<Void> migrate() {
    return createSchemaHistoryTableIfNotExists()
      .compose(empty -> runMigrations().mapEmpty());
  }

  private Future<Void> createSchemaHistoryTableIfNotExists() {
    return vertx.fileSystem()
      .readFile("flyway_schema_history_ddl.sql")
      .compose(buffer -> sqlClient.query(buffer.toString()).execute().mapEmpty());
  }

  private Future<MigrationOutput> runMigrations() {
    return vertx.fileSystem()
      .readDir(migrationPaths.get(0))
      .compose(migrationScripts ->
        migrationScripts.stream()
          .map(path ->
            loadMigrationScript(path)
              .compose(this::executeMigration)
              .compose(this::updateHistoryTable)
          )
          .reduce((a, b) -> a.compose(x -> b))
          .orElse(Future.succeededFuture())
      );
  }

  private Future<SqlMigrationScript> loadMigrationScript(String path) {
    return vertx.fileSystem()
      .readFile(path)
      .map(buffer -> {
        String sql = buffer.toString();
        int checksum = Checksums.calculateChecksum(sql);
        String filename = new File(path).getName();
        return new SqlMigrationScript(
          path,
          filename,
          sql,
          Paths.getDescriptionFromFilename(filename),
          checksum,
          SqlMigrationScript.Category.fromChar(Paths.getCategoryFromFilename(filename)),
          Paths.getVersionFromFilename(filename)
        );
      });
  }

  private Future<MigrationOutput> executeMigration(SqlMigrationScript script) {
    long startTime = System.currentTimeMillis();
    return sqlClient.query(script.sql())
      .execute()
      .map(x -> new MigrationOutput(
          script,
          System.currentTimeMillis() - startTime
        )
      );
  }

  private Future<MigrationOutput> updateHistoryTable(MigrationOutput migrationOutput) {
    SqlMigrationScript script = migrationOutput.script();
    String sql = """
      insert into flyway_schema_history(installed_rank, version, description, type,\
      script, checksum, installed_by, installed_on, execution_time, success)\
      values((select coalesce(max(installed_rank), 0) + 1 from flyway_schema_history),\
      $1, $2, 'SQL', $3, $4, (select current_user), $5, $6, $7)""";
    Tuple tuple = Tuple.of(
      script.version(),
      script.description(),
      script.filename(),
      script.checksum(),
      LocalDateTime.now(),
      migrationOutput.executionTime(),
      true
    );
    return sqlClient.preparedQuery(sql)
      .execute(tuple)
      .map(migrationOutput);
  }
}
