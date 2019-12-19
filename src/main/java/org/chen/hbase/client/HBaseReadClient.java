package org.chen.hbase.client;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.chen.hbase.Get;

public interface HBaseReadClient {

  public boolean exists(String tableName, Get get) throws IOException;
  public boolean[] existsAll(String tableName, List<Get> gets) throws IOException;

  /**
   * query one column with primary key
   * @param tableName target table create on the cluster
   * @param rowkey the primary key to find
   * @param cf the column famaliy to query
   * @param col the target column
   * @return the column value
   */
  public String get(String tableName, String rowkey, String cf, String col) throws IOException;
  public Result get(String tableName, Get get) throws IOException;
  public Result[] get(String tableName, List<Get> gets) throws IOException;

  /**
   * make sure to close the ResultScanner at finally <br>
   * Deprecated use {@link HBaseWriteClient#scan(String, Scan, ResultCallback)} instead
   */
  @Deprecated
  public ResultScanner getScanner(String tableName, Scan scan) throws IOException;
  public void scan(String tableName, Scan scan, ResultCallback callback) throws IOException;
}
