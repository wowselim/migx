package co.selim.migx.core;

public interface ThrowingFunction<T, R> {
  R apply(T t) throws Throwable;
}
