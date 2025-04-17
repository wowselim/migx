package co.selim.migx.core;

import co.selim.migx.core.impl.SqlClientMigx;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlClient;

import java.util.Arrays;
import java.util.List;

public interface Migx {
  Future<Void> migrate();

  static Migx create(Vertx vertx, SqlClient sqlClient) {
    return new SqlClientMigx(vertx, sqlClient);
  }

  static Migx create(Vertx vertx, SqlClient sqlClient, List<String> migrationPath) {
    return new SqlClientMigx(vertx, sqlClient, migrationPath);
  }

  static Migx create(Vertx vertx, SqlClient sqlClient, String... migrationPaths) {
    return create(vertx, sqlClient, Arrays.asList(migrationPaths));
  }
}
