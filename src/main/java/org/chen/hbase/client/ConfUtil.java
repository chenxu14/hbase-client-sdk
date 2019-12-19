package org.chen.hbase.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.chen.hbase.util.ConfigClient;
import org.chen.hbase.util.MyConstants;

/**
 * Util class for interact with MCC
 */
public class ConfUtil {
  private static final Log LOG = LogFactory.getLog(HBaseWriteClient.class);
  private static volatile String customConfFile;
  private static volatile Configuration customConf;

  static ConfigClient initConfClient(String clientId, String appKey){
    // TODO return ConfigClient instance
    return null;
  }

  /**
   * parse clusterName from json
   */
  static TableInfo parseTableInfo(String tableName, String json) throws IOException {
    if (json == null){
      throw new IOException("cluster json is null, tableName is " + tableName);
    }
    JSONParser parser=new JSONParser();
    Object obj;
    try {
      obj = parser.parse(json);
    } catch (ParseException e) {
      LOG.error(e.getMessage());
      throw new IOException("parse clusterName failed!");
    }
    JSONArray array = (JSONArray)obj;
    JSONObject clusterObj = (JSONObject)array.get(0);
    String clusterName = clusterObj.get("clusterName").toString();
    Object backupCluster = clusterObj.get("backupCluster");
    Object realName = clusterObj.get("realName");
    Object partitions = clusterObj.get("partitions");
    Object failover = clusterObj.get("failover");
    return new TableInfo(clusterName,
        backupCluster == null ? null : backupCluster.toString(),
        realName == null ? null : realName.toString(),
        partitions == null ? 1 : Integer.parseInt(partitions.toString()),
        failover == null ? false : "true".equals(failover.toString()));
  }

  /**
   * parse zkInfo from json, first is zkserver, second is zkport
   */
  static ClusterInfo parseClusterInfo(String cluster, String json) throws IOException {
    if(json == null){
      throw new IOException("cluster json is null, cluster is : " + cluster);
    }
    JSONParser parser=new JSONParser();
    JSONObject obj;
    try {
      obj = (JSONObject) parser.parse(json);
      Object name = obj.get("name");
      Object zkServers = obj.get("zkServers");
      Object zkPort = obj.get("zkPort");
      Object state = obj.get("state");
      Object kafkaServers = obj.get("kafkaServers");
      return new ClusterInfo(name == null ? null : name.toString(),
          zkServers == null ? null : zkServers.toString(),
          zkPort == null ? null : zkPort.toString(),
          state == null ? null : state.toString(),
          kafkaServers == null ? null : kafkaServers.toString());
    } catch (ParseException e) {
      LOG.error(e.getMessage());
      throw new IOException("parse zkinfo failed!");
    }
  }

  /**
   * if hbase cluster managed, init kerberos conf 
   */
  static void initHBaseEnv(Configuration conf, ConfigClient mtcli)
      throws IOException {
    System.setProperty("io.netty.leakDetection.level", "DISABLED");
    if (mtcli != null) {
      String krbConfFile = System.getProperty("java.security.krb5.conf");
      if(krbConfFile == null || "".equals(krbConfFile.trim()) || !new File(krbConfFile).exists()) {
        // no krb5.conf setup, download it from MCC
        String krbConfPath = conf.get(MyConstants.KRB5_CONF_PATH, MyConstants.KRB5_CONF_DEFAULT);
        if(!krbConfPath.endsWith(Path.SEPARATOR)){
          krbConfFile = krbConfPath + Path.SEPARATOR + "krb5.conf";
        } else {
          krbConfFile = krbConfPath + "krb5.conf";
        }
        String krb5conf = mtcli.getValue("krbconf");
        FileUtils.writeStringToFile(new File(krbConfFile), krb5conf);
        System.setProperty("java.security.krb5.conf", krbConfFile);
        LOG.info("krb5.conf's path is " + krbConfFile + ", and content is :" + krb5conf);
      }
    }

    conf.set("hbase.security.authentication", "kerberos");
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hbase.security.authorization", "true");
    conf.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
    if(conf.get("hbase.regionserver.kerberos.principal") == null){
      conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@REALM.COM");
    }
    if(conf.get("hbase.master.kerberos.principal") == null){
      conf.set("hbase.master.kerberos.principal", "hbase/_HOST@REALM.COM");
    }

    // compatible with old cloudtable version
    if(conf.get("hbase.app.keytab.file") != null && conf.get("hbase.client.keytab.file") == null){
      conf.set("hbase.client.keytab.file", conf.get("hbase.app.keytab.file"));
      conf.set("hbase.client.kerberos.principal", conf.get("hbase.app.kerberos.principal"));
    }
    if(conf.get("hbase.app.keytab.password") != null && conf.get("hbase.client.keytab.password") == null){
      conf.set("hbase.client.keytab.password", conf.get("hbase.app.keytab.password"));
      conf.set("hbase.client.kerberos.principal", conf.get("hbase.app.kerberos.principal"));
    }

    // no hbase.client.keytab.file & hbase.client.keytab.password
    // use default keytab and principal
    if(conf.get("hbase.client.keytab.file") == null && conf.get("hbase.client.keytab.password") == null){
      conf.set("hbase.client.keytab.file", "/etc/hadoop/keytabs/hbase-default.keytab");
      conf.set("hbase.client.kerberos.principal", "hbase-default/_HOST@REALM.COM");
    }
    UserGroupInformation.setConfiguration(conf);
  }

  static Configuration initConf() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // compatible with old cloudtable version
    conf.addResource("hbase-client.xml");
    conf.addResource("hbase-auth.xml");
    if(customConfFile != null && !"".equals(customConfFile.trim())) {
      LOG.info("load custome conf file : " + customConfFile);
      conf.addResource(new FileInputStream(customConfFile));
    }
    if (customConf != null) {
      LOG.info("load custome Configuration");
      conf.addResource(customConf);
    }
    conf.set("hbase.client.connection.impl", "org.apache.hadoop.hbase.client.CompositeConnection");
    // change default value
    if (conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER)
        == HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER) {
      conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
    }
    if (conf.getInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT)
        == HConstants.DEFAULT_HBASE_RPC_TIMEOUT) {
      conf.setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, 10000);
    }
    if (conf.getInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT)
        == HConstants.DEFAULT_HBASE_RPC_TIMEOUT) {
      conf.setInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY, 10000);
    }
    if (conf.getInt(HConstants.HBASE_CLIENT_MAX_PERSERVER_TASKS, HConstants.DEFAULT_HBASE_CLIENT_MAX_PERSERVER_TASKS)
        == HConstants.DEFAULT_HBASE_CLIENT_MAX_PERSERVER_TASKS) {
      conf.setInt(HConstants.HBASE_CLIENT_MAX_PERSERVER_TASKS, 10);
    }
    if (conf.getInt(HConstants.HBASE_CLIENT_MAX_PERREGION_TASKS, HConstants.DEFAULT_HBASE_CLIENT_MAX_PERREGION_TASKS)
        == HConstants.DEFAULT_HBASE_CLIENT_MAX_PERREGION_TASKS) {
      conf.setInt(HConstants.HBASE_CLIENT_MAX_PERREGION_TASKS, 10);
    }
    if (conf.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING)
        == HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING) {
      conf.setInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, 100);
    }
    return conf;
  }

  /**
   * shoud be called before HBaseClientImpl.getInstance(), if you want custome conf to be load<br>
   * when HBaseClientImpl instance has created, this method has no effect any more
   * @param confFile the local path of the config file
   */
  public static void setCustomConf(String confFile){
    customConfFile = confFile;
  }

  public static void setCustomConf(Configuration conf){
    customConf = conf;
  }
}
