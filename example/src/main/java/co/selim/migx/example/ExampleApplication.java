package co.selim.migx.example;

import co.selim.migx.core.Migx;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class ExampleApplication {

  private static Migx getMigx(Vertx vertx, JdbcDatabaseContainer<?> container) {
    String jdbcUrl = container.getJdbcUrl();
    String username = container.getUsername();
    String password = container.getPassword();

    SqlConnectOptions connectOptions = SqlConnectOptions.fromUri(jdbcUrl.substring("jdbc:".length()))
      .setUser(username)
      .setPassword(password);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(4);
    Pool client = Pool.pool(vertx, connectOptions, poolOptions);

    return Migx.create(vertx, client);
  }

  public static void main(String[] args) {
    try (JdbcDatabaseContainer<?> pgContainer = new PostgreSQLContainer<>("postgres:17.5-alpine")) {
      pgContainer.start();
      Vertx vertx = Vertx.vertx();
      Migx migx = getMigx(vertx, pgContainer);
      migx.migrate()
        .onSuccess(migrations -> System.out.println("Successfully ran " + migrations.size() + " migrations."))
        .onFailure(t -> {
          System.err.println("Failed to run migrations");
          t.printStackTrace(System.err);
        })
        .eventually(vertx::close)
        .toCompletionStage()
        .toCompletableFuture()
        .join();
    }
  }
}
