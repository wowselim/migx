package co.selim.migx.core;

import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.flywaydb.core.api.output.MigrateOutput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class MigrationTest extends IntegrationTest {

  private final Vertx vertx;

  public MigrationTest(Vertx vertx) {
    this.vertx = vertx;
  }

  private List<MigrateOutput> migrateFlyway(JdbcDatabaseContainer<?> container, List<String> locations) {
    return getFlyway(container, locations.toArray(String[]::new)).migrate().migrations;
  }

  private List<MigrationOutput> migrateMigx(JdbcDatabaseContainer<?> container, List<String> locations) {
    try {
      return getMigx(vertx, container, locations)
        .migrate()
        .toCompletionStage()
        .toCompletableFuture()
        .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @MethodSource("dbArguments")
  @DisplayName("The migration history tables match")
  void migrationHistoriesMatch(JdbcDatabaseContainer<?> flywayContainer, JdbcDatabaseContainer<?> migxContainer) {
    withContainers(flywayContainer, migxContainer, () -> {
      List<String> migrationPaths = List.of("db/migration");
      migrateFlyway(flywayContainer, migrationPaths);
      migrateMigx(migxContainer, migrationPaths);
      List<SchemaHistoryEntry> flywaySchemaHistory = getSchemaHistory(flywayContainer);
      List<SchemaHistoryEntry> migxSchemaHistory = getSchemaHistory(migxContainer);
      assertIterableEquals(flywaySchemaHistory, migxSchemaHistory);
    });
  }

  @ParameterizedTest
  @MethodSource("dbArguments")
  @DisplayName("Duplicate files are ignored")
  void duplicateFilesAreIgnored(JdbcDatabaseContainer<?> flywayContainer, JdbcDatabaseContainer<?> migxContainer) {
    withContainers(flywayContainer, migxContainer, () -> {
      List<String> migrationPaths = List.of("db/migration", "db/migration");
      migrateFlyway(flywayContainer, migrationPaths);
      migrateMigx(migxContainer, migrationPaths);
    });
  }

  @ParameterizedTest
  @MethodSource("dbArguments")
  @DisplayName("Migrations fail if duplicate versions exist")
  void migrationsFailIfDuplicateVersionsExist(JdbcDatabaseContainer<?> flywayContainer, JdbcDatabaseContainer<?> migxContainer) {
    withContainers(flywayContainer, migxContainer, () -> {
      List<String> migrationPaths = List.of("db/duplicate-versions");
      Throwable flywayError = Assertions.assertThrows(Throwable.class, () -> {
        migrateFlyway(flywayContainer, migrationPaths);
      });
      Throwable migxError = Assertions.assertThrows(Throwable.class, () -> {
        migrateMigx(migxContainer, migrationPaths);
      });

      String message = "Found more than one migration with version 1.2";
      assertTrue(flywayError.getMessage().contains(message));
      assertTrue(migxError.getMessage().contains(message));
    });
  }

  @ParameterizedTest
  @MethodSource("dbArguments")
  @DisplayName("Listing a path twice only runs the migrations once")
  void listingPathTwiceRunsMigrationsOnlyOnce(JdbcDatabaseContainer<?> flywayContainer, JdbcDatabaseContainer<?> migxContainer) {
    withContainers(flywayContainer, migxContainer, () -> {
      List<String> migrationPaths = List.of("db/migration", "db/migration");
      List<MigrateOutput> flywayMigrations = migrateFlyway(flywayContainer, migrationPaths);
      List<MigrationOutput> migxMigrations = migrateMigx(migxContainer, migrationPaths);

      assertEquals(2, flywayMigrations.size());
      assertEquals(flywayMigrations.size(), migxMigrations.size());
    });
  }

  @ParameterizedTest
  @MethodSource("dbArguments")
  @DisplayName("Migrations can be run twice with no effect")
  void migrationsCanBeRunTwiceWithNoEffect(JdbcDatabaseContainer<?> flywayContainer, JdbcDatabaseContainer<?> migxContainer) {
    withContainers(flywayContainer, migxContainer, () -> {
      List<String> migrationPaths = List.of("db/migration");
      List<MigrateOutput> flywayMigrationsFirstRun = migrateFlyway(flywayContainer, migrationPaths);
      List<MigrationOutput> migxMigrationsFirstRun = migrateMigx(migxContainer, migrationPaths);
      List<MigrateOutput> flywayMigrationsSecondRun = migrateFlyway(flywayContainer, migrationPaths);
      List<MigrationOutput> migxMigrationsSecondRun = migrateMigx(migxContainer, migrationPaths);

      assertEquals(2, flywayMigrationsFirstRun.size());
      assertEquals(2, migxMigrationsFirstRun.size());
      assertEquals(0, flywayMigrationsSecondRun.size());
      assertEquals(0, migxMigrationsSecondRun.size());
    });
  }
}
