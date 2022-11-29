package co.selim.migx.core;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

public record SchemaHistoryEntry(
  int installedRank,
  String version,
  String description,
  String type,
  String script,
  long checksum,
  String installedBy,
  LocalDateTime installedOn,
  long executionTime,
  boolean success
) {
  public static final class Mapper implements RowMapper<SchemaHistoryEntry> {
    @Override
    public SchemaHistoryEntry map(ResultSet rs, StatementContext ctx) throws SQLException {
      int installedRank = rs.getInt("installed_rank");
      String version = rs.getString("version");
      String description = rs.getString("description");
      String type = rs.getString("type");
      String script = rs.getString("script");
      long checksum = rs.getLong("checksum");
      String installedBy = rs.getString("installed_by");
      LocalDateTime installedOn = rs.getTimestamp("installed_on").toLocalDateTime();
      long executionTime = rs.getLong("execution_time");
      boolean success = rs.getBoolean("success");
      return new SchemaHistoryEntry(
        installedRank,
        version,
        description,
        type,
        script,
        checksum,
        installedBy,
        installedOn,
        executionTime,
        success
      );
    }
  }
}
