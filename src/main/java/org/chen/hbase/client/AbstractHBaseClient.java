package org.chen.hbase.client;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.PasswdAuthChore;
import org.apache.hadoop.security.UserGroupInformation;
import org.chen.hbase.Get;
import org.chen.hbase.Put;
import org.chen.hbase.util.ConfigClient;
import org.chen.hbase.util.IConfigChangeListener;
import org.chen.hbase.util.MyConstants;

public abstract class AbstractHBaseClient implements IConfigChangeListener, HBaseClient {
  private static final Log LOG = LogFactory.getLog(HBaseClient.class);

  protected final Map<String, TableInfo> tables; // tableName -> TableInfo
  protected final Map<String, ClusterInfo> clusters; // clusterName -> ClusterInfo
  protected final ChoreService choreService;
  protected final Configuration conf;
  protected final boolean hasWatcher;
  protected ConfigClient mtcli;
  protected volatile boolean isStopped = false;

  protected AbstractHBaseClient() throws IOException {
    tables = new ConcurrentHashMap<>();
    clusters = new ConcurrentHashMap<>();
    choreService = new ChoreService("CloudTable");
    conf = ConfUtil.initConf();
    this.hasWatcher = conf.getBoolean(MyConstants.CLIENT_WATCH_TABLE, true);

    String appKey = conf.get("hbase.client.sdk.appkey", MyConstants.APPKEY);
    if (appKey == null || "".equals(appKey.trim())) {
      throw new IOException("hbase.client.sdk.appkey can't be null.");
    }
    mtcli = ConfUtil.initConfClient("mthbase.client-" + UUID.randomUUID().toString(), appKey);
    if (hasWatcher) {
      mtcli.addListener("krbconf", this);
    }
    ConfUtil.initHBaseEnv(conf, mtcli);

    initAuthChore();
    initHotTable();
    registerShutdownHook();
  }

  @Override
  public void changed(String node, String oldValue, String newValue) {
    if (node.equals("krbconf")) {
      try {
        String krbConfFile = System.getProperty("java.security.krb5.conf");
        FileUtils.writeStringToFile(new File(krbConfFile), newValue);
      } catch (IOException e) {
        LOG.error("save krb5.conf to local fail!", e);
      }
    }
  }

  /**
   * setup connections and warm up region locations that configured in hbase.client.hottable
   */
  protected void initHotTable() throws IOException {
    String hottables = conf.get(MyConstants.CLIENT_HOTTABLE);
    if (hottables != null && !"".equals(hottables)) {
      for (String tableName : hottables.split(",")) {
        initTable(tableName.trim());
      }
      LOG.info("successfully init hot tables: " + hottables);
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  protected void initCluster(String clusterName) throws IOException {
    boolean hasCluster = this.clusters.containsKey(clusterName);
    if (!hasCluster) {
      synchronized (this) {
        hasCluster = this.clusters.containsKey(clusterName);
        if (!hasCluster) {
          String jsonInfo = mtcli.getValue(MyConstants.CLUSTER_PREFIX + clusterName);
          ClusterInfo clusterInfo = ConfUtil.parseClusterInfo(clusterName, jsonInfo);
          this.clusters.put(clusterName, clusterInfo);
          if (this.hasWatcher) {
            mtcli.addListener(MyConstants.CLUSTER_PREFIX + clusterName, this);
          }
        }
      }
    }
  }

  protected void initTable(String tableName) throws IOException {
    boolean hasTable = tables.containsKey(tableName);
    if (!hasTable) {
      synchronized (this) {
        hasTable = tables.containsKey(tableName);
        if (!hasTable) {
          String tableNode = tableName.replaceFirst(":", "_");
          String jsonInfo = mtcli.getValue(MyConstants.TABLE_PREFIX + tableNode);
          TableInfo tableInfo = ConfUtil.parseTableInfo(tableName, jsonInfo);
          tables.put(tableName, tableInfo);
          if (this.hasWatcher) {
            mtcli.addListener(MyConstants.TABLE_PREFIX + tableNode, this);
          }
        }
      }
    }
  }

  private void registerShutdownHook() {
    Thread hook = new Thread("CloudTableHook") {
      @Override
      public void run() {
        if (!isStopped){
          AbstractHBaseClient.this.stop("JVM exit!");
        }
      }
    };
    Runtime.getRuntime().addShutdownHook(hook);
  }

  /**
   * start deamon thread period refresh TGT from kerberos
   */
  private void initAuthChore() throws IOException {
    String authType = conf.get("hbase.client.authentication");
    if ("token".equals(authType)) {
      LOG.info("auth with token, tokenSize:" +
          UserGroupInformation.getCurrentUser().getCredentials().getAllTokens().size());
      return;
    }
    String passwd = conf.get("hbase.client.keytab.password");
    if (passwd != null && !"".equals(passwd.trim())) {
      int period = conf.getInt(MyConstants.CLIENT_AUTH_PERIOD, MyConstants.CLIENT_AUTH_DEFAULT);
      PasswdAuthChore authChore = new PasswdAuthChore(conf, this, period);
      choreService.scheduleChore(authChore);
      return;
    }
    String keytab = conf.get("hbase.client.keytab.file");
    if (keytab == null) {
      throw new IOException("no auth type find, should set hbase.client.keytab.password or hbase.client.keytab.file.");
    }
    ScheduledChore authChore = AuthUtil.getAuthChore(conf);
    if (authChore != null) {
      choreService.scheduleChore(authChore);
    }
    LOG.info("init auth chore sunccessfully, login user is " +
        UserGroupInformation.getLoginUser().getUserName());
  }

  public void stop(String why) {
    if (isStopped) {
      return;
    }
    this.choreService.shutdown();
    if (mtcli != null) {
      this.mtcli.close();
    }
    close();
    this.isStopped = true;
    LOG.info("HBaseClient successfully stoped, reason is " + why);
  }

  protected abstract void close();

  @Override
  public boolean isStopped() {
    return isStopped;
  }

  public void put(String tableName, String cf, String rowkey, List<Pair<String, String>> values)
      throws IOException {
    Put put = new Put(rowkey);
    for (Pair<String, String> value : values) {
      put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(value.getFirst()), Bytes.toBytes(value.getSecond()));
    }
    this.put(tableName, put);
  }

  public void putAsync(String tableName, String cf, String rowkey,
      List<Pair<String, String>> values) throws IOException {
    Put p = new Put(rowkey);
    for (Pair<String, String> value : values) {
      p.addColumn(Bytes.toBytes(cf), Bytes.toBytes(value.getFirst()),
          Bytes.toBytes(value.getSecond()));
    }
    this.putAsync(tableName, p);
  }

  @Override
  public String get(String tableName, String rowkey, String cf, String col) throws IOException {
    throw new UnsupportedOperationException("get operation not supported.");
  }

  @Override
  public Result get(String tableName, Get get) throws IOException {
    throw new UnsupportedOperationException("get operation not supported.");
  }

  @Override
  public Result[] get(String tableName, List<Get> gets) throws IOException {
    throw new UnsupportedOperationException("get operation not supported.");
  }

  @Override
  @Deprecated
  public ResultScanner getScanner(String tableName, Scan scan) throws IOException {
    throw new UnsupportedOperationException("scan operation not supported.");
  }

  @Override
  public void scan(String tableName, Scan scan, ResultCallback callback) throws IOException {
    throw new UnsupportedOperationException("scan operation not supported.");
  }

  @Override
  public Result increment(String tableName, Increment increment) throws IOException {
    throw new UnsupportedOperationException("increment operation not supported.");
  }

  @Override
  public boolean exists(String tableName, Get get) throws IOException {
    throw new UnsupportedOperationException("exists operation not supported.");
  }

  @Override
  public boolean[] existsAll(String tableName, List<Get> gets) throws IOException {
    throw new UnsupportedOperationException("existsAll operation not supported.");
  }

  @Override
  public boolean checkAndPut(String tableName, String row, String family, String qualifier,
      byte[] value, Put put) throws IOException {
    throw new UnsupportedOperationException("checkAndPut operation not supported.");
  }

  @Override
  public boolean checkAndPut(String tableName, String row, String family, String qualifier,
      CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
    throw new UnsupportedOperationException("checkAndPut operation not supported.");
  }

  @Override
  public void batch(String tableName, final List<? extends Row> actions, final Object[] results)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("batch operation not supported.");
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(String tableName, byte[] row) throws IOException {
    throw new UnsupportedOperationException("coprocessorService operation not supported.");
  }

  @Override
  public Result append(String tableName, Append append) throws IOException {
    throw new UnsupportedOperationException("append operation not supported.");
  }

  @Override
  public AggregationClient getAggregationClient() {
    throw new UnsupportedOperationException("Aggregation operation not supported.");  
  }

  protected void exceptionCallback(Exception e) throws IOException {
    // do some exception callback
    throw new IOException(e);
  }
}
