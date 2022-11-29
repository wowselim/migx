package co.selim.migx.core;

import co.selim.migx.core.output.MigrationResult;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.flywaydb.core.api.output.MigrateResult;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

@ExtendWith(VertxExtension.class)
public class MigrationTest extends IntegrationTest {

  private MigrateResult flywayResult;
  private MigrationResult migxResult;

  @BeforeEach
  void migrate(Vertx vertx) {
    flywayResult = getFlyway().migrate();
    migxResult = getMigx(vertx).migrate();
  }

  @Test
  void x() {
    List<SchemaHistoryEntry> flywaySchemaHistory = getSchemaHistory(flywayJdbi);
    // TODO: List<SchemaHistoryEntry> migxSchemaHistory = getSchemaHistory(migxJdbi);
  }

  private List<SchemaHistoryEntry> getSchemaHistory(Jdbi jdbi) {
    return jdbi.withHandle(handle -> {
      String query = "select * from flyway_schema_history order by installed_rank";
      return handle.createQuery(query)
        .mapTo(SchemaHistoryEntry.class)
        .list();
    });
  }
}
