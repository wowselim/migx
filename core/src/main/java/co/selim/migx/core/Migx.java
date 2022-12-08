package co.selim.migx.core;

import co.selim.migx.core.impl.SqlClientMigx;
import co.selim.migx.core.output.MigrationResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlClient;

public interface Migx {
  Future<MigrationResult> migrate();

  static Migx create(Vertx vertx, SqlClient sqlClient) {
    return new SqlClientMigx(vertx, sqlClient);
  }
}
