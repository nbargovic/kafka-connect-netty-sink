package io.confluent.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;

public class SourceHeader implements Header {

  private String key;
  private String value;

  public SourceHeader(String key, String value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String key() {
    return this.key;
  }

  @Override
  public Schema schema() {
    return null;
  }

  @Override
  public Object value() {
    return this.value;
  }

  @Override
  public Header with(Schema schema, Object value) {
    this.value = (String) value;
    return this;
  }

  @Override
  public Header rename(String key) {
    this.key = key;
    return this;
  }
}
