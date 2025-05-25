package co.selim.migx.core.impl.util;

import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.internal.pool.CloseablePool;

import static co.selim.migx.core.impl.util.Pools.Implementation.MYSQL;
import static co.selim.migx.core.impl.util.Pools.Implementation.POSTGRES;

public final class Pools {

  private Pools() {
  }

  public enum Implementation {
    POSTGRES, MYSQL
  }

  public static Implementation identify(Pool pool) {
    if (pool instanceof CloseablePool closeablePool) {
      return identifyCloseablePool(closeablePool.driver().getClass().getName());
    } else {
      throw new IllegalArgumentException("Unknown pool implementation: " + pool.getClass().getName());
    }
  }

  private static Implementation identifyCloseablePool(String className) {
    if (className.contains("MySQLDriver")) {
      return MYSQL;
    }
    if (className.contains("PgDriver")) {
      return POSTGRES;
    }
    throw new IllegalArgumentException("Unknown pool implementation: " + className);
  }
}
