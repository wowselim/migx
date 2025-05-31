package co.selim.migx.core;

import org.testcontainers.containers.JdbcDatabaseContainer;

public record ContainerConfiguration(
  JdbcDatabaseContainer<?> flywayContainer,
  JdbcDatabaseContainer<?> migxContainer
) {

  public ContainerConfiguration {
    if (!flywayContainer.getDockerImageName().equals(migxContainer.getDockerImageName())) {
      throw new IllegalArgumentException(
        "Incompatible images [" + flywayContainer.getDockerImageName() + ", " + migxContainer.getDockerImageName() + "]"
      );
    }
  }

  @Override
  public String toString() {
    return "image=" + flywayContainer.getDockerImageName();
  }
}
