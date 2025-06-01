package co.selim.migx.core;

import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.output.MigrateOutput;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
public abstract class IntegrationTest {

  protected final Vertx vertx;
  protected final JdbcDatabaseContainer<?> flywayContainer;
  protected final JdbcDatabaseContainer<?> migxContainer;

  protected IntegrationTest(ContainerConfiguration containerConfiguration) {
    this.vertx = Vertx.vertx();
    this.flywayContainer = containerConfiguration.flywayContainer();
    this.migxContainer = containerConfiguration.migxContainer();
  }

  @BeforeEach
  void startup() {
    CompletableFuture.allOf(
      CompletableFuture.runAsync(flywayContainer::start),
      CompletableFuture.runAsync(migxContainer::start)
    ).join();
  }

  @AfterEach
  void teardown() {
    CompletableFuture.allOf(
      CompletableFuture.runAsync(flywayContainer::stop),
      CompletableFuture.runAsync(migxContainer::stop)
    ).join();
    vertx.close()
      .toCompletionStage()
      .toCompletableFuture()
      .join();
  }

  private static JdbcDatabaseContainer<?> pgContainer() {
    return new PostgreSQLContainer<>("postgres:17.5-alpine");
  }

  private static JdbcDatabaseContainer<?> mySqlContainer() {
    return new MySQLContainer<>("mysql:8.0.36");
  }

  protected static Stream<Arguments> dbArguments() {
    return Stream.of(
      arguments(new ContainerConfiguration(pgContainer(), pgContainer())),
      arguments(new ContainerConfiguration(mySqlContainer(), mySqlContainer()))
    );
  }

  protected List<SchemaHistoryEntry> getSchemaHistory(JdbcDatabaseContainer<?> container) {
    return withConnection(container, connection -> {
      String query = "select * from flyway_schema_history order by installed_rank";
      try (ResultSet resultSet = connection.createStatement().executeQuery(query)) {
        return SchemaHistoryEntry.MAPPER.apply(resultSet);
      }
    });
  }

  private <T> T withConnection(JdbcDatabaseContainer<?> container, ThrowingFunction<Connection, T> function) {
    try (Connection connection = container.createConnection("")) {
      return function.apply(connection);
    } catch (Throwable e) {
      Assertions.fail(e);
      throw new RuntimeException(e);
    }
  }

  protected Flyway getFlyway(JdbcDatabaseContainer<?> container, String... locations) {
    String jdbcUrl = container.getJdbcUrl();
    String username = container.getUsername();
    String password = container.getPassword();

    return new FluentConfiguration()
      .dataSource(jdbcUrl, username, password)
      .locations(locations)
      .load();
  }

  private Migx getMigx(JdbcDatabaseContainer<?> container, List<String> locations) {
    String jdbcUrl = container.getJdbcUrl();
    String username = container.getUsername();
    String password = container.getPassword();

    SqlConnectOptions connectOptions = SqlConnectOptions.fromUri(jdbcUrl.substring("jdbc:".length()))
      .setUser(username)
      .setPassword(password);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(4);
    Pool client = Pool.pool(vertx, connectOptions, poolOptions);

    return Migx.create(vertx, client, locations);
  }

  protected List<MigrateOutput> migrateFlyway(JdbcDatabaseContainer<?> container, List<String> locations) {
    return getFlyway(container, locations.toArray(String[]::new)).migrate().migrations;
  }

  protected List<MigrationOutput> migrateMigx(JdbcDatabaseContainer<?> container, List<String> locations) {
    try {
      return getMigx(container, locations)
        .migrate()
        .toCompletionStage()
        .toCompletableFuture()
        .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
