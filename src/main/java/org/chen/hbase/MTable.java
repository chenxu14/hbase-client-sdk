package org.chen.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.CompositeConnection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.chen.hbase.util.ConnectionCallable;
import org.chen.hbase.util.OperationNotSupportedException;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class MTable implements Table {
  private final Table activeTable;
  private Optional<Table> backupTable;
  private CompositeConnection conn;

  public MTable(final CompositeConnection conn, final TableName tableName) throws IOException {
    this.activeTable = conn.getActiveConnection().getTable(tableName);
    Table backupTable = null;
    if (conn.getBackupConnection().isPresent()) {
      backupTable = conn.getBackupConnection().get().getTable(tableName);
    }
    this.backupTable = Optional.ofNullable(backupTable);
    this.conn = conn;
  }

  public Result get(final Get get) throws IOException {
    List<ConnectionCallable<Result>> tasks = new ArrayList<ConnectionCallable<Result>>();
    tasks.add(new ConnectionCallable<Result>(true) {
      @Override
      public Result call() throws Exception {
        return activeTable.get(get);
      }
    });
    if (backupTable.isPresent()) {
      tasks.add(new ConnectionCallable<Result>(false) {
        @Override
        public Result call() throws Exception {
          return backupTable.get().get(get);
        }
      });
    }
    try {
      return conn.getThreadPool().invokeAny(tasks);
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    List<ConnectionCallable<Boolean>> tasks = new ArrayList<ConnectionCallable<Boolean>>();
    tasks.add(new ConnectionCallable<Boolean>(true) {
      @Override
      public Boolean call() throws Exception {
        return activeTable.exists(get);
      }
    });
    if (backupTable.isPresent()) {
      tasks.add(new ConnectionCallable<Boolean>(false) {
        @Override
        public Boolean call() throws Exception {
          return backupTable.get().exists(get);
        }
      });
    }
    try {
      return conn.getThreadPool().invokeAny(tasks);
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public TableName getName() {
    return activeTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return activeTable.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return activeTable.getTableDescriptor();
  }

  @Override
  public boolean[] existsAll(List<? extends Get> gets) throws IOException {
    List<ConnectionCallable<boolean[]>> tasks = new ArrayList<ConnectionCallable<boolean[]>>();
    tasks.add(new ConnectionCallable<boolean[]>(true) {
      @Override
      public boolean[] call() throws Exception {
        return activeTable.existsAll(gets);
      }
    });
    if (backupTable.isPresent()) {
      tasks.add(new ConnectionCallable<boolean[]>(false) {
        @Override
        public boolean[] call() throws Exception {
          return backupTable.get().existsAll(gets);
        }
      });
    }
    try {
      return conn.getThreadPool().invokeAny(tasks);
	} catch (InterruptedException | ExecutionException e) {
      throw new IOException(e.getMessage(), e);
	}
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
    throw new OperationNotSupportedException("batch operation not supported yet.");
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    throw new OperationNotSupportedException("batch operation not supported yet.");
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    throw new OperationNotSupportedException("batchCallback operation not supported yet.");
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback)
      throws IOException, InterruptedException {
    throw new OperationNotSupportedException("batchCallback operation not supported yet.");
  }

  @Override
  public Result[] get(List<? extends Get> gets) throws IOException {
    List<ConnectionCallable<Result[]>> tasks = new ArrayList<ConnectionCallable<Result[]>>();
    tasks.add(new ConnectionCallable<Result[]>(true) {
      @Override
      public Result[] call() throws Exception {
        return activeTable.get(gets);
      }
    });
    if (backupTable.isPresent()) {
      tasks.add(new ConnectionCallable<Result[]>(false) {
        @Override
        public Result[] call() throws Exception {
          return backupTable.get().get(gets);
        }
      });
    }
    try {
      return conn.getThreadPool().invokeAny(tasks);
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return activeTable.getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return activeTable.getScanner(family);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return activeTable.getScanner(family, qualifier);
  }

  @Override
  public void put(Put put) throws IOException {
    List<ConnectionCallable<Void>> tasks = new ArrayList<ConnectionCallable<Void>>();
    tasks.add(new ConnectionCallable<Void>(true) {
      @Override
      public Void call() throws Exception {
        activeTable.put(put);
        return null;
      }
    });
    if (backupTable.isPresent()) {
      tasks.add(new ConnectionCallable<Void>(false) {
        @Override
        public Void call() throws Exception {
          backupTable.get().put(put);
          return null;
        }
      });
    }
    try {
	  conn.getThreadPool().invokeAll(tasks);
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void put(List<? extends Put> puts) throws IOException {
    List<ConnectionCallable<Void>> tasks = new ArrayList<ConnectionCallable<Void>>();
    tasks.add(new ConnectionCallable<Void>(true) {
      @Override
      public Void call() throws Exception {
        activeTable.put(puts);
        return null;
      }
    });
    if (backupTable.isPresent()) {
      tasks.add(new ConnectionCallable<Void>(false) {
        @Override
        public Void call() throws Exception {
          backupTable.get().put(puts);
          return null;
        }
      });
    }
    try {
      conn.getThreadPool().invokeAll(tasks);
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
    throw new OperationNotSupportedException("checkAndPut operation not supported yet.");
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, Put put)
      throws IOException {
    throw new OperationNotSupportedException("checkAndPut operation not supported yet.");
  }

  @Override
  public void delete(Delete delete) throws IOException {
    List<ConnectionCallable<Void>> tasks = new ArrayList<ConnectionCallable<Void>>();
    tasks.add(new ConnectionCallable<Void>(true) {
      @Override
      public Void call() throws Exception {
        activeTable.delete(delete);
        return null;
      }
    });
    if (backupTable.isPresent()) {
      tasks.add(new ConnectionCallable<Void>(false) {
        @Override
        public Void call() throws Exception {
          backupTable.get().delete(delete);
          return null;
        }
      });
    }
    try {
      conn.getThreadPool().invokeAll(tasks);
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void delete(List<? extends Delete> deletes) throws IOException {
    List<ConnectionCallable<Void>> tasks = new ArrayList<ConnectionCallable<Void>>();
    tasks.add(new ConnectionCallable<Void>(true) {
      @Override
      public Void call() throws Exception {
        activeTable.delete(deletes);
        return null;
      }
    });
    if (backupTable.isPresent()) {
      tasks.add(new ConnectionCallable<Void>(false) {
        @Override
        public Void call() throws Exception {
          backupTable.get().delete(deletes);
          return null;
        }
      });
    }
    try {
      conn.getThreadPool().invokeAll(tasks);
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
      throws IOException {
    throw new OperationNotSupportedException("checkAndDelete operation not supported yet.");
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value,
      Delete delete) throws IOException {
    throw new OperationNotSupportedException("checkAndDelete operation not supported yet.");
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    List<ConnectionCallable<Void>> tasks = new ArrayList<ConnectionCallable<Void>>();
    tasks.add(new ConnectionCallable<Void>(true) {
      @Override
      public Void call() throws Exception {
        activeTable.mutateRow(rm);
        return null;
      }
    });
    if (backupTable.isPresent()) {
      tasks.add(new ConnectionCallable<Void>(false) {
        @Override
        public Void call() throws Exception {
          backupTable.get().mutateRow(rm);
          return null;
        }
      });
    }
    try {
      conn.getThreadPool().invokeAll(tasks);
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public Result append(Append append) throws IOException {
    throw new OperationNotSupportedException("append operation not supported yet.");
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    throw new OperationNotSupportedException("increment operation not supported yet.");
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
    throw new OperationNotSupportedException("incrementColumnValue operation not supported yet.");
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
      throws IOException {
    throw new OperationNotSupportedException("incrementColumnValue operation not supported yet.");
  }

  @Override
  public void close() throws IOException {
    activeTable.close();
    if (backupTable.isPresent()) {
      backupTable.get().close();
    }
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return null;
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
      Call<T, R> callable) throws ServiceException, Throwable {
    throw new OperationNotSupportedException("coprocessorService operation not supported yet.");
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
      Call<T, R> callable, Callback<R> callback) throws ServiceException, Throwable {
    throw new OperationNotSupportedException("coprocessorService operation not supported yet.");
  }

  @Override
  public long getWriteBufferSize() {
    return conn.getConnectionConfiguration().getWriteBufferSize();
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(MethodDescriptor methodDescriptor,
      Message request, byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
    throw new OperationNotSupportedException("batchCoprocessorService operation not supported yet.");
  }

  @Override
  public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor, Message request,
      byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
      throws ServiceException, Throwable {
    throw new OperationNotSupportedException("batchCoprocessorService operation not supported yet.");
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value,
      RowMutations mutation) throws IOException {
    throw new OperationNotSupportedException("checkAndMutate operation not supported yet.");
  }

  @Override
  public int getOperationTimeout() {
    return conn.getConnectionConfiguration().getOperationTimeout();
  }

  @Override
  public int getRpcTimeout() {
    return conn.getConnectionConfiguration().getReadRpcTimeout();
  }

  @Override
  public int getReadRpcTimeout() {
    return conn.getConnectionConfiguration().getReadRpcTimeout();
  }

  @Override
  public int getWriteRpcTimeout() {
    return conn.getConnectionConfiguration().getWriteRpcTimeout();
  }

  public Table getActiveHTable() {
    return this.activeTable;
  }

  public Optional<Table> getBackupHTable() {
    return this.backupTable;
  }

  @Override
  public void setOperationTimeout(int operationTimeout) {}
  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {}
  @Override
  public void setRpcTimeout(int rpcTimeout) {}
  @Override
  public void setReadRpcTimeout(int readRpcTimeout) {}
  @Override
  public void setWriteRpcTimeout(int writeRpcTimeout) {}
}
