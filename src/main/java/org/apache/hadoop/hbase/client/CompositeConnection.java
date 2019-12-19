package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Threads;
import org.chen.hbase.MTable;
import org.chen.hbase.util.ConnectionExecutorService;
import org.chen.hbase.util.OperationNotSupportedException;

public class CompositeConnection implements Connection {
  private static final Log LOG = LogFactory.getLog(CompositeConnection.class);
  private final Connection activeCluster;
  private Optional<Connection> backupCluster;
  private final ExecutorService connThreadPool;
  private final ConnectionConfiguration connConf;
  private final Configuration conf;

  CompositeConnection(Configuration conf, boolean manage, ExecutorService pool, User user)
      throws IOException {
    this.conf = conf;
    conf.set("hbase.client.connection.impl", "org.apache.hadoop.hbase.client.ConnectionManager$HConnectionImplementation");
    ExecutorService primary = newBatchPool("Primary");
    ExecutorService secondary = null;
    activeCluster = ConnectionFactory.createConnection(conf, primary, user);
    Connection backupCluster = null;
    if (conf.get(HConstants.ZOOKEEPER_QUORUM + ".backup") != null) {
      Configuration clusterConf = new Configuration(conf);
      clusterConf.set(HConstants.ZOOKEEPER_QUORUM, conf.get(HConstants.ZOOKEEPER_QUORUM + ".backup"));
      clusterConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, conf.get(HConstants.ZOOKEEPER_CLIENT_PORT + ".backup"));
      secondary = newBatchPool("Secondary");
      backupCluster = ConnectionFactory.createConnection(clusterConf, secondary, user);
      LOG.info("backup cluster connection created.");
    }
    this.connThreadPool = new ConnectionExecutorService(primary, secondary);
    this.backupCluster = Optional.ofNullable(backupCluster);
    connConf = new ConnectionConfiguration(conf);
  }

  private ExecutorService newBatchPool(String name) {
    int maxThreads = conf.getInt("hbase.hconnection.threads.max", 0);
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors();
    }
    int coreThreads = conf.getInt("hbase.hconnection.threads.core", 0);
    if (coreThreads == 0) {
      coreThreads = Runtime.getRuntime().availableProcessors();
    }
    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(coreThreads, maxThreads, 60, TimeUnit.SECONDS,
        workQueue, Threads.newDaemonThreadFactory(name + "-shared-"));
    tpe.allowCoreThreadTimeOut(true);
    return tpe;
  }

  @Override
  public void abort(String why, Throwable e) {
    activeCluster.abort(why, e);
    backupCluster.ifPresent(conn -> conn.abort(why, e));
  }

  @Override
  public boolean isAborted() {
    boolean activeAborted = activeCluster.isAborted();
    boolean backupAborted = backupCluster.map(conn -> conn.isAborted()).orElse(true);
    return activeAborted && backupAborted;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    return new MTable(this, tableName);
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
  	return getTable(tableName);
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return getBufferedMutator(new BufferedMutatorParams(tableName).pool(connThreadPool));
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    return null;
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    throw new OperationNotSupportedException("getRegionLocator operation not supported yet.");
  }

  @Override
  public Admin getAdmin() throws IOException {
    throw new OperationNotSupportedException("getRegionLocator operation not supported yet.");
  }

  @Override
  public void close() throws IOException {
    if (activeCluster != null) {
      activeCluster.close();
    }
    if (backupCluster.isPresent()) {
      backupCluster.get().close();
    }
  }

  @Override
  public boolean isClosed() {
    boolean activeClosed = activeCluster == null ? true : activeCluster.isClosed();
    boolean backupClosed = backupCluster.map(conn -> conn.isClosed()).orElse(true);
    return activeClosed && backupClosed;
  }

  public Connection getActiveConnection() {
    return activeCluster;
  }

  public Optional<Connection> getBackupConnection() {
    return backupCluster;
  }

  public ExecutorService getThreadPool() {
    return this.connThreadPool;
  }

  public ConnectionConfiguration getConnectionConfiguration() {
    return this.connConf;
  }

  public RpcControllerFactory getRpcControllerFactory() {
    return ((ClusterConnection)getActiveConnection()).getRpcControllerFactory();
  }

  public RpcRetryingCallerFactory getRpcRetryingCallerFactory() {
    return ((ClusterConnection)getActiveConnection()).getRpcRetryingCallerFactory();
  }

  public void cacheRegionLocations(TableName tableName) throws IOException {
    this.activeCluster.getRegionLocator(tableName).getAllRegionLocations();
    if (this.backupCluster.isPresent()) {
      this.backupCluster.get().getRegionLocator(tableName).getAllRegionLocations();
    }
  }
}
