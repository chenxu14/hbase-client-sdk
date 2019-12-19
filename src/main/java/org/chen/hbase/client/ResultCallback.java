package org.chen.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.client.ResultScanner;

public interface ResultCallback {
  public void callWithScanner(ResultScanner scanner) throws IOException;
}
