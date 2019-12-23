package org.chen.hbase.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.chen.hbase.Delete;
import org.chen.hbase.Get;
import org.chen.hbase.Put;

public abstract class BaseOP extends AbstractHBaseClient {
  protected BaseOP() throws IOException {
    super();
  }

  abstract Table getTable(String tableName) throws IOException;

  abstract BufferedMutator getBufferedMutator(String tableName) throws IOException;

  @Override
  public void put(String tableName, Put put) throws IOException {
    Table table = getTable(tableName);
    try {
      table.put(put);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
  }

  @Override
  public void putAsync(String tableName, Put put) throws IOException {
    BufferedMutator mutator = getBufferedMutator(tableName);
    try {
      mutator.mutate(put);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    }
  }

  @Override
  public void put(String tableName, List<Put> puts) throws IOException {
    Table table = getTable(tableName);
    try {
      table.put(puts);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
  }

  @Override
  public void putAsync(String tableName, List<Put> puts) throws IOException {
    BufferedMutator mutator = getBufferedMutator(tableName);
    try {
      mutator.mutate(puts);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    }
  }

  public String get(String tableName, String rowkey, String cf, String col) throws IOException {
    Get get = new Get(rowkey);
    get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col));
    Result res = this.get(tableName, get);
    Cell cell = (res == null) ? null : res.getColumnLatestCell(Bytes.toBytes(cf), Bytes.toBytes(col));
    return cell == null ? null : Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  @Override
  public Result get(String tableName, Get get) throws IOException {
    long startTime = System.currentTimeMillis();
    Table table = getTable(tableName);
    Result res = null;
    try {
      res = table.get(get);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
      long duration = System.currentTimeMillis() - startTime;
      int cells = res.rawCells().length;
      metricsChore.updateRpc(tableName, cells == 0 ? duration : duration / cells);
    }
    return res;
  }

  @Override
  public Result[] get(String tableName, List<Get> gets) throws IOException {
    long startTime = System.currentTimeMillis();
    Table table = getTable(tableName);
    Result[] res = null;
    try {
      res = table.get(gets);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
      long duration = System.currentTimeMillis() - startTime;
      int cells = 0;
      for (Result r : res) {
        cells += r.rawCells().length;
      }
      metricsChore.updateRpc(tableName, cells == 0 ? duration : duration / cells);
    }
    return res;
  }

  @Override
  public void delete(String tableName, Delete delete) throws IOException {
    Table table = getTable(tableName);
    try {
      table.delete(delete);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
  }

  @Override
  public void delete(String tableName, List<Delete> deletes) throws IOException {
    Table table = getTable(tableName);
    try {
      table.delete(deletes);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
  }

  @Override
  @Deprecated
  public ResultScanner getScanner(String tableName, Scan scan) throws IOException {
    Table table = getTable(tableName);
    ResultScanner res = null;
    try {
      res = table.getScanner(scan);
    } finally {
      table.close();
    }
    return res;
  }

  @Override
  public void scan(String tableName, Scan scan, ResultCallback callback) throws IOException {
    Table table = getTable(tableName);
    try (ResultScanner res = table.getScanner(scan)) {
      callback.callWithScanner(res);
    } finally {
      table.close();
    }
  }

  @Override
  public Result increment(String tableName, Increment increment) throws IOException {
    Table table = getTable(tableName);
    Result res = null;
    try {
      res = table.increment(increment);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
    return res;
  }

  @Override
  public boolean exists(String tableName, Get get) throws IOException {
    Table table = getTable(tableName);
    boolean res = false;
    try {
      res = table.exists(get);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
    return res;
  }

  @Override
  public boolean[] existsAll(String tableName, List<Get> gets) throws IOException {
    Table table = getTable(tableName);
    boolean[] res = null;
    try {
      res = table.existsAll(gets);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
    return res;
  }

  @Override
  public boolean checkAndPut(String tableName, String row, String family, String qualifier,
      byte[] value, Put put) throws IOException {
    Table table = getTable(tableName);
    boolean res = false;
    try {
      res = table.checkAndPut(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier),
          value, put);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
    return res;
  }

  @Override
  public boolean checkAndPut(String tableName, String row, String family, String qualifier,
      CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
    Table table = getTable(tableName);
    boolean res = false;
    try {
      res = table.checkAndPut(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier), compareOp,
          value, put);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
    return res;
  }

  @Override
  public void batch(String tableName, final List<? extends Row> actions, final Object[] results)
      throws IOException, InterruptedException {
    Table table = getTable(tableName);
    try {
      table.batch(actions, results);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
  }

  @Override
  public Result append(String tableName, Append append) throws IOException {
    Table table = getTable(tableName);
    Result res = null;
    try {
      res = table.append(append);
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    } finally {
      table.close();
    }
    return res;
  }

  @Override
  public void flushCommits(String tableName) throws IOException {
    getBufferedMutator(tableName).flush();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(String tableName, byte[] row) throws IOException {
    Table table = getTable(tableName);
    CoprocessorRpcChannel chanel = null;
    try {
      chanel = table.coprocessorService(row);
    } finally {
      table.close();
    }
    return chanel;
  }
}
