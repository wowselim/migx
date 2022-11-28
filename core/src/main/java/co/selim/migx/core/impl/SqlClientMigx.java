package co.selim.migx.core.impl;

import co.selim.migx.core.Migx;
import co.selim.migx.core.output.MigrationResult;
import io.vertx.sqlclient.SqlClient;

public class SqlClientMigx implements Migx {

  private final SqlClient sqlClient;

  public SqlClientMigx(SqlClient sqlClient) {
    this.sqlClient = sqlClient;
  }

  @Override
  public MigrationResult migrate() {
    return null;
  }
}
