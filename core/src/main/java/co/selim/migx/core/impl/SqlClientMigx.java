package co.selim.migx.core.impl;

import co.selim.migx.core.Migx;
import co.selim.migx.core.output.MigrationResult;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlClient;

public class SqlClientMigx implements Migx {

  private final Vertx vertx;
  private final SqlClient sqlClient;

  public SqlClientMigx(Vertx vertx, SqlClient sqlClient) {
    this.vertx = vertx;
    this.sqlClient = sqlClient;
  }

  @Override
  public MigrationResult migrate() {
    return null;
  }
}
