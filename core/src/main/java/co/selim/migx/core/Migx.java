package co.selim.migx.core;

import co.selim.migx.core.impl.SqlClientMigx;
import co.selim.migx.core.output.MigrationResult;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlClient;

public interface Migx {
  MigrationResult migrate();

  static Migx create(Vertx vertx, SqlClient sqlClient) {
    return new SqlClientMigx(vertx, sqlClient);
  }
}
