package co.selim.migx.core;

import co.selim.migx.core.impl.PoolMigx;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface Migx {

  Future<List<MigrationOutput>> migrate();

  static Migx create(Vertx vertx, Pool pool) {
    return new PoolMigx(vertx, pool);
  }

  static Migx create(Vertx vertx, Pool pool, List<String> migrationPath) {
    return new PoolMigx(vertx, pool, migrationPath);
  }

  static Migx create(Vertx vertx, Pool pool, String migrationPath, String... additionalMigrationPaths) {
    List<String> allPaths = new ArrayList<>(additionalMigrationPaths.length + 1);
    allPaths.add(migrationPath);
    allPaths.addAll(Arrays.asList(additionalMigrationPaths));
    return create(vertx, pool, allPaths);
  }
}
