package co.selim.migx.core.impl.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

public final class Checksums {

  private Checksums() {
  }

  public static int calculateChecksum(String input) {
    CRC32 crc32 = new CRC32();

    try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
      String line;
      while ((line = reader.readLine()) != null) {
        crc32.update(line.getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return (int) crc32.getValue();
  }
}
