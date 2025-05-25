package co.selim.migx.core;

import co.selim.migx.core.impl.PoolMigx;
import co.selim.migx.core.impl.runner.MigrationRunner;
import co.selim.migx.core.impl.runner.MySQLMigrationRunner;
import co.selim.migx.core.impl.runner.PgMigrationRunner;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static co.selim.migx.core.impl.util.Pools.identify;

public interface Migx {

  Future<List<MigrationOutput>> migrate();

  static Migx create(Vertx vertx, Pool pool) {
    return create(vertx, pool, List.of("db/migration"));
  }

  static Migx create(Vertx vertx, Pool pool, List<String> migrationPath) {
    MigrationRunner migrationRunner = switch (identify(pool)) {
      case POSTGRES -> new PgMigrationRunner(vertx, pool);
      case MYSQL -> new MySQLMigrationRunner(vertx, pool);
    };
    return new PoolMigx(vertx, migrationPath, migrationRunner);
  }

  static Migx create(Vertx vertx, Pool pool, String migrationPath, String... additionalMigrationPaths) {
    List<String> allPaths = new ArrayList<>(additionalMigrationPaths.length + 1);
    allPaths.add(migrationPath);
    allPaths.addAll(Arrays.asList(additionalMigrationPaths));
    return create(vertx, pool, allPaths);
  }
}
