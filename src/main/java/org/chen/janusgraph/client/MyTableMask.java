package org.chen.janusgraph.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.janusgraph.diskstorage.hbase.TableMask;

public class MyTableMask implements TableMask {
  private final Table table;

  public MyTableMask(Table table) {
    this.table = table;
  }

  @Override
  public void close() throws IOException {
    table.close();
  }

  @Override
  public ResultScanner getScanner(Scan filter) throws IOException {
    return table.getScanner(filter);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return table.get(gets);
  }

  @Override
  public void batch(List<Row> writes, Object[] results)
      throws IOException, InterruptedException {
    table.batch(writes, results);
  }

}
