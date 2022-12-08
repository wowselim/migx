package co.selim.migx.core.impl;

import co.selim.migx.core.Migx;
import co.selim.migx.core.output.MigrationResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.sqlclient.SqlClient;

public class SqlClientMigx implements Migx {

  private final Vertx vertx;
  private final SqlClient sqlClient;

  public SqlClientMigx(Vertx vertx, SqlClient sqlClient) {
    this.vertx = vertx;
    this.sqlClient = sqlClient;
  }

  @Override
  public Future<MigrationResult> migrate() {
    var migrationFiles = vertx.fileSystem()
      .readDir("db/migration")
      .map(migrationScripts ->
        migrationScripts.stream()
          .map(script ->
            vertx.fileSystem()
              .readFile(script)
              .map(sqlClient.query(script).execute())
          )
          .toList()
      );
      /*
      .map(migrations ->
        migrations.stream()
          .<Supplier<Future<Buffer>>>map(migration -> () -> vertx.fileSystem().readFile(migration))
          .toList()
      )
      .compose(x ->
        x
      );
       */


    return null;
  }
}
