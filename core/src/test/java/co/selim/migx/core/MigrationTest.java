package co.selim.migx.core;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static co.selim.migx.core.IntegrationTest.Database.FLYWAY;
import static co.selim.migx.core.IntegrationTest.Database.MIGX;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

@ExtendWith(VertxExtension.class)
public class MigrationTest extends IntegrationTest {

  private final Vertx vertx;

  public MigrationTest(Vertx vertx) {
    this.vertx = vertx;
  }

  private void migrate(List<String> locations, Database... databases) {
    for (Database db : databases) {
      if (db == FLYWAY) {
        getFlyway(locations.toArray(String[]::new)).migrate();
      } else {
        CountDownLatch latch = new CountDownLatch(1);
        Future<Void> result = getMigx(vertx, locations)
          .migrate()
          .onComplete(x -> latch.countDown());
        try {
          latch.await();
          if (result.failed()) {
            throw new RuntimeException(result.cause());
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Test
  @DisplayName("The migration history tables match")
  void migrationHistoriesMatch() {
    migrate(List.of("db/migration"), Database.values());
    List<SchemaHistoryEntry> flywaySchemaHistory = getSchemaHistory(FLYWAY);
    List<SchemaHistoryEntry> migxSchemaHistory = getSchemaHistory(MIGX);
    assertIterableEquals(flywaySchemaHistory, migxSchemaHistory);
  }

  @ParameterizedTest
  @EnumSource(Database.class)
  @DisplayName("Migrations fail when table already exists")
  void migrationsFailWhenTableAlreadyExists(Database db) {
    Assertions.assertThrows(Throwable.class, () -> {
      migrate(List.of("db/migration", "db/migration2"), db);
    });
  }
}
