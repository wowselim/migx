package co.selim.migx.core.impl.util;

public final class Clock {

  private Clock() {
  }

  public static long now() {
    return System.currentTimeMillis();
  }

  public static long millisSince(long time) {
    return now() - time;
  }
}
