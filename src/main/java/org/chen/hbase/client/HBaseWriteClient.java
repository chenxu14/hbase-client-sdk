package org.chen.hbase.client;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Pair;

import org.chen.hbase.Delete;
import org.chen.hbase.Put;

public interface HBaseWriteClient {
  /**
   * insert one row with some columns
   * @param tableName target table create on the cluster
   * @param cf the column famaliy
   * @param rowkey the primary key value
   * @param values some &lt;column,value> pairs
   */
  public void put(String tableName, String cf, String rowkey, List<Pair<String,String>> values) throws IOException;
  public void put(String tableName, Put put) throws IOException;
  public void put(String tableName, List<Put> put) throws IOException;

  public void putAsync(String tableName, String cf, String rowkey, List<Pair<String,String>> values) throws IOException;
  public void putAsync(String tableName, Put put) throws IOException;
  public void putAsync(String tableName, List<Put> put) throws IOException;
  public void flushCommits(String tableName) throws IOException;

  public void delete(String tableName, Delete delete) throws IOException;
  public void delete(String tableName, List<Delete> deletes) throws IOException;

  public Result increment(String tableName, Increment increment) throws IOException;
  public Result append(String tableName, Append append) throws IOException;
  
  /**
   * use Bytes.toBytes to convert value to bytes
   * @throws IOException
   */
  public boolean checkAndPut(String tableName, String row, String family, String qualifier, byte[] value, Put put) throws IOException;

  /**
   * use Bytes.toBytes to convert value to bytes
   * @throws IOException
   */
  public boolean checkAndPut(String tableName, String row, String family, String qualifier, CompareFilter.CompareOp compareOp, 
      byte[] value, Put put) throws IOException;
}
