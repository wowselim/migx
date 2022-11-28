package co.selim.migx.core;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class IntegrationTest {

  private static final String PG_IMAGE_NAME = "postgres:14.3";

  @Container
  protected PostgreSQLContainer<?> flywayContainer = new PostgreSQLContainer<>(PG_IMAGE_NAME);

  @Container
  protected PostgreSQLContainer<?> migxContainer = new PostgreSQLContainer<>(PG_IMAGE_NAME);
}
