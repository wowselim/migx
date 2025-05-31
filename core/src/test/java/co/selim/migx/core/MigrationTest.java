package co.selim.migx.core;

import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.flywaydb.core.api.output.MigrateOutput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MigrationTest {

  @Nested
  @ParameterizedClass
  @ExtendWith(VertxExtension.class)
  @MethodSource("co.selim.migx.core.IntegrationTest#dbArguments")
  class Tests extends IntegrationTest {

    protected Tests(ContainerConfiguration containerConfiguration) {
      super(Vertx.vertx(), containerConfiguration);
    }

    @Test
    @DisplayName("The migration history tables match")
    void migrationHistoriesMatch() {
      List<String> migrationPaths = List.of("db/migration");
      migrateFlyway(flywayContainer, migrationPaths);
      migrateMigx(migxContainer, migrationPaths);
      List<SchemaHistoryEntry> flywaySchemaHistory = getSchemaHistory(flywayContainer);
      List<SchemaHistoryEntry> migxSchemaHistory = getSchemaHistory(migxContainer);
      assertIterableEquals(flywaySchemaHistory, migxSchemaHistory);
    }

    @Test
    @DisplayName("Duplicate files are ignored")
    void duplicateFilesAreIgnored() {
      List<String> migrationPaths = List.of("db/migration", "db/migration");
      migrateFlyway(flywayContainer, migrationPaths);
      migrateMigx(migxContainer, migrationPaths);
    }

    @Test
    @DisplayName("Migrations fail if duplicate versions exist")
    void migrationsFailIfDuplicateVersionsExist() {
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
    }

    @Test
    @DisplayName("Listing a path twice only runs the migrations once")
    void listingPathTwiceRunsMigrationsOnlyOnce() {
      List<String> migrationPaths = List.of("db/migration", "db/migration");
      List<MigrateOutput> flywayMigrations = migrateFlyway(flywayContainer, migrationPaths);
      List<MigrationOutput> migxMigrations = migrateMigx(migxContainer, migrationPaths);

      assertEquals(2, flywayMigrations.size());
      assertEquals(flywayMigrations.size(), migxMigrations.size());
    }

    @Test
    @DisplayName("Migrations can be run twice with no effect")
    void migrationsCanBeRunTwiceWithNoEffect() {
      List<String> migrationPaths = List.of("db/migration");
      List<MigrateOutput> flywayMigrationsFirstRun = migrateFlyway(flywayContainer, migrationPaths);
      List<MigrationOutput> migxMigrationsFirstRun = migrateMigx(migxContainer, migrationPaths);
      List<MigrateOutput> flywayMigrationsSecondRun = migrateFlyway(flywayContainer, migrationPaths);
      List<MigrationOutput> migxMigrationsSecondRun = migrateMigx(migxContainer, migrationPaths);

      assertEquals(2, flywayMigrationsFirstRun.size());
      assertEquals(2, migxMigrationsFirstRun.size());
      assertEquals(0, flywayMigrationsSecondRun.size());
      assertEquals(0, migxMigrationsSecondRun.size());
    }
  }
}
