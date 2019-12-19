package org.chen.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CompositeConnection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User.SecureHadoopUser;
import org.apache.hadoop.security.UserGroupInformation;
import org.chen.hbase.Delete;
import org.chen.hbase.Put;
import org.chen.hbase.util.MyConstants;

/**
 * HBase client interact with Server synchronous
 */
public class HBaseClientImpl extends BaseOP {
  private static final Log LOG = LogFactory.getLog(HBaseWriteClient.class);
  private static HBaseClientImpl instance; // singleton format
  private static boolean instanced = false;
  private volatile boolean failover = false;
  private Map<String, Connection> connPool; // clusterName -> Connection
  private Map<String, BufferedMutator> mutators; // tableName -> clusterName
  private Map<String, EmbedKafkaClient> kafkas; // tableName -> KafkaClient

  private HBaseClientImpl() throws IOException {
    super();
  }

  /**
   * setup connections and warm up region locations that configured in hbase.client.hottable
   */
  protected void initHotTable() throws IOException {
    connPool = new ConcurrentHashMap<>();
    mutators = new ConcurrentHashMap<>();
    kafkas = new ConcurrentHashMap<>();
    String hottables = conf.get(MyConstants.CLIENT_HOTTABLE);
    if (hottables != null && !"".equals(hottables)) {
      for (String table : hottables.split(",")) {
        this.initTable(table);
        TableInfo tableInfo = tables.get(table);
        CompositeConnection conn = (CompositeConnection) getConnection(tableInfo);
        String realName = tableInfo.getRealName() != null ? tableInfo.getRealName() : table;
        conn.cacheRegionLocations(TableName.valueOf(realName));
        LOG.info("successfully init hot tables: " + hottables);
      }
    }
  }

  public static HBaseClientImpl getInstance() {
    if (instance == null) {
      synchronized (HBaseClientImpl.class) {
        if (instance == null) {
          try {
            instance = new HBaseClientImpl();
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
          instanced = true;
        }
      }
    }
    return instance;
  }

  public static boolean isInstanced() {
    return instanced;
  }

  EmbedKafkaClient getKafkaClient(String tableName) throws IOException {
    boolean hasClient = kafkas.containsKey(tableName);
    if (!hasClient) {
      synchronized (this) {
        hasClient = kafkas.containsKey(tableName);
        if (!hasClient) {
          this.initTable(tableName);
          String clusterName = this.tables.get(tableName).getClusterName();
          this.initCluster(clusterName);
          String kafkaServers = this.clusters.get(clusterName).getKafkaServers();
          EmbedKafkaClient kafkaClient = new EmbedKafkaClient(this, kafkaServers);
          kafkas.put(tableName, kafkaClient);
        }
      }
    }
    return kafkas.get(tableName);
  }

  /**
   * if the target Table instance exist the return it, or create it and put it back to cache.<br>
   * if managed, method have race condition with TableChangeListener<br>
   * make sure to call the Table.close() at the end, in order to Releases any resources held by this table
   */
  Table getTable(String tableName) throws IOException {
    this.initTable(tableName);
    TableInfo table = tables.get(tableName);
    Connection conn = getConnection(table);
    String realName = table.getRealName() != null ? table.getRealName() : tableName;
    return conn.getTable(TableName.valueOf(realName));
  }

  BufferedMutator getBufferedMutator(String tableName) throws IOException {
    boolean hasMutator = mutators.containsKey(tableName);
    if (!hasMutator) {
      synchronized (mutators) {
        if (!hasMutator) {
          this.initTable(tableName);
          TableInfo table = tables.get(tableName);
          Connection conn = getConnection(table);
          String realName = table.getRealName() != null ? table.getRealName() : tableName;
          mutators.put(tableName, conn.getBufferedMutator(TableName.valueOf(realName)));
        }
      }
    }
    return mutators.get(tableName);
  }

  /**
   * if target conn cached then return it, or create it and put it to the cache
   */
  Connection getConnection(TableInfo table) throws IOException {
    String cluster = table.getClusterName();
    if (cluster == null) {
      throw new IOException("error get connection, cluster name can't be null.");
    }
    Connection conn = null;
    boolean hasConn = connPool.containsKey(cluster); 
    if (hasConn) {
      conn = connPool.get(cluster);
      if (conn != null && !conn.isClosed()) {
        return conn;
      } else { // connection closed, reconnect it
        synchronized (this) {
          conn = connPool.get(cluster);
          if (conn != null && !conn.isClosed()) {
            return conn;
          } else {
            LOG.warn("Connection to " + cluster + "closed, reconnect to it again!");
            connPool.remove(cluster);
          }
        }
      }
    }
    // connection closed or has not created
    synchronized (this) {
      hasConn = connPool.containsKey(cluster);
      if (!hasConn) {
        Configuration clusterConf = new Configuration(conf);
        this.initCluster(cluster);
        ClusterInfo clusterInfo = clusters.get(cluster);
        clusterConf.set(HConstants.ZOOKEEPER_QUORUM, clusterInfo.getZkServers());
        clusterConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, clusterInfo.getZkPort());
        clusterConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
        String backupCluster = table.getBackupCluster();
        if (backupCluster != null) {
          this.initCluster(backupCluster);
          clusterInfo = clusters.get(backupCluster);
          clusterConf.set(HConstants.ZOOKEEPER_QUORUM + ".backup", clusterInfo.getZkServers());
          clusterConf.set(HConstants.ZOOKEEPER_CLIENT_PORT + ".backup", clusterInfo.getZkPort());
        }
        SecureHadoopUser user = new SecureHadoopUser(UserGroupInformation.getLoginUser());
        conn = ConnectionFactory.createConnection(clusterConf, user);
        connPool.put(cluster, conn);
      } else {
        conn = connPool.get(cluster);
      }
    }
    return conn;
  }

  @Override
  public void changed(String node, String oldValue, String newValue) {
    super.changed(node, oldValue, newValue);
    try {
      if (node.startsWith(MyConstants.TABLE_PREFIX)) {
        TableInfo tableInfo = ConfUtil.parseTableInfo(node, newValue);
        this.failover = tableInfo.isFailover();
        String table = node.substring(node.indexOf("_") + 1);
        Connection conn = getConnection(tableInfo);
        String tableName = table.replaceFirst("_", ":"); // mcc node do not support ':'
        if (tables.containsKey(table)) {
          tables.put(table, tableInfo);
        } else if (tables.containsKey(tableName)) {
          tables.put(tableName, tableInfo);
        }
        if (mutators.containsKey(table)) {
          String realName = tableInfo.getRealName() != null ? tableInfo.getRealName() : table;
          BufferedMutator mutator = conn.getBufferedMutator(TableName.valueOf(realName));
          BufferedMutator preValue = mutators.put(table, mutator);
          preValue.flush();
          preValue.close();
        }
        LOG.info("change " + table +"'s flow to " + tableInfo.getClusterName());
      } else if (node.startsWith(MyConstants.CLUSTER_PREFIX)) {
        String cluster = node.substring(node.indexOf("_") + 1);
        ClusterInfo clusterInfo = ConfUtil.parseClusterInfo(cluster, newValue);
        clusters.put(cluster, clusterInfo);
        LOG.info("cluster " + cluster + "'s state has changed, current state is : " + clusterInfo.getState());  
      }
    } catch (Exception e) {
      LOG.info("recreate table error!", e);
    }
  }

  @Override
  public AggregationClient getAggregationClient() {
    return new AggregationClient(this);
  }

  /**
   * when this method called, connection to hbase will close,
   * and no operation can performed
   */
  @Override
  protected void close() {
    for (Connection conn : connPool.values()) {
      try {
        conn.close();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
    for (BufferedMutator mutator : mutators.values()) {
      try {
        mutator.close();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
    for (EmbedKafkaClient kafka : kafkas.values()) {
      kafka.close();
    }
  }

  @Override
  public void flushCommits(String tableName) throws IOException {
    if (failover) {
      super.flushCommits(tableName);
    } else {
      getKafkaClient(tableName).flushCommits(tableName);
    }
  }

  public void put(String tableName, Put put) throws IOException {
    if (failover) {
      super.put(tableName, put);
    } else {
      getKafkaClient(tableName).put(tableName, put);
    }
  }

  public void putAsync(String tableName, Put put) throws IOException {
    if (failover) {
      super.putAsync(tableName, put);
    } else {
      getKafkaClient(tableName).putAsync(tableName, put);
    }
  }

  public void put(String tableName, List<Put> puts) throws IOException {
    if (failover) {
      super.put(tableName, puts);
    } else {
      getKafkaClient(tableName).put(tableName, puts);
    }
  }

  public void putAsync(String tableName, List<Put> puts) throws IOException {
    if (failover) {
      super.putAsync(tableName, puts);
    } else {
      getKafkaClient(tableName).putAsync(tableName, puts);
    }
  }

  public void delete(String tableName, Delete delete) throws IOException {
	if (failover) {
      super.delete(tableName, delete);
    } else {
      getKafkaClient(tableName).delete(tableName, delete);
    }
  }

  public void delete(String tableName, List<Delete> deletes) throws IOException {
    if (failover) {
      super.delete(tableName, deletes);
    } else {
      getKafkaClient(tableName).delete(tableName, deletes);
    }
  }
}
