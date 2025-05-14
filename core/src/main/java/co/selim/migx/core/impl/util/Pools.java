package co.selim.migx.core.impl.util;

import io.vertx.sqlclient.Pool;

public final class Pools {

  private Pools() {
  }

  public enum Implementation {
    POSTGRES, MYSQL
  }

  public static Implementation identify(Pool pool) {
    String className = pool.getClass().getName();
    if (className.contains("MySQLPoolImpl")) {
      return Implementation.MYSQL;
    }
    if (className.contains("PgPoolImpl")) {
      return Implementation.POSTGRES;
    }
    throw new IllegalArgumentException("Unknown pool implementation: " + className);
  }
}
