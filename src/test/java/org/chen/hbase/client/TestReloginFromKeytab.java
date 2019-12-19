package org.chen.hbase.client;

import java.io.File;
import java.net.BindException;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.chen.hbase.Put;

@Category(SmallTests.class)
public class TestReloginFromKeytab {
  private static final Log LOG = LogFactory.getLog(TestReloginFromKeytab.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final File KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri().getPath());
  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String hbasePrincipal;
  private static String spnegoPrincipal;
  private static HBaseWriteClient client;
  private static final String tableName = "testtable";

  @BeforeClass
  public static void setUp() throws Exception {
    KDC = setupMiniKdc();
    KDC.createPrincipal(KEYTAB_FILE, "chenxu14/" + HOST, "HTTP/" + HOST);
    hbasePrincipal = "chenxu14/" + HOST + "@" + KDC.getRealm();
    spnegoPrincipal = "HTTP/" + HOST + "@" + KDC.getRealm();
    Configuration conf = TEST_UTIL.getConfiguration();
    // hbase security conf
    conf.set("hbase.master.keytab.file", KEYTAB_FILE.getAbsolutePath());
    conf.set("hbase.master.kerberos.principal", hbasePrincipal);
    conf.set("hbase.regionserver.keytab.file", KEYTAB_FILE.getAbsolutePath());
    conf.set("hbase.regionserver.kerberos.principal", hbasePrincipal);
    conf.set("hbase.security.authentication", "kerberos");
    conf.setBoolean("hbase.security.authorization", true);
    conf.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
    conf.setBoolean("hbase.security.exec.permission.checks", true);
    conf.set("hbase.superuser", "hbase");
    // hadoop security conf
    conf.set("hadoop.security.authentication", "kerberos");
    conf.setBoolean("hadoop.security.authorization", true);
    conf.set("hadoop.http.authentication.type", "kerberos");
    conf.set("hadoop.security.auth_to_local", "RULE:[1:$1] RULE:[2:$1]");
    conf.setBoolean("dfs.block.access.token.enable", true);
    conf.set("dfs.permissions.superusergroup", "hbase");
    conf.set("dfs.namenode.kerberos.principal", hbasePrincipal);
    conf.set("dfs.namenode.keytab.file", KEYTAB_FILE.getAbsolutePath());
    conf.set("dfs.datanode.kerberos.principal", hbasePrincipal);
    conf.set("dfs.datanode.keytab.file", KEYTAB_FILE.getAbsolutePath());
    conf.set("dfs.web.authentication.kerberos.principal", spnegoPrincipal);
    conf.set("dfs.web.authentication.kerberos.keytab", KEYTAB_FILE.getAbsolutePath());
    conf.set("dfs.journalnode.keytab.file", KEYTAB_FILE.getAbsolutePath());
    conf.set("dfs.journalnode.kerberos.principal", hbasePrincipal);
    conf.set("dfs.journalnode.kerberos.internal.spnego.principal", spnegoPrincipal);
    conf.set("dfs.cluster.administrators", "hbase");
    conf.set("dfs.namenode.kerberos.internal.spnego.principal", spnegoPrincipal);
    conf.setBoolean(DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY, true);
    conf.set(DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    TEST_UTIL.startMiniCluster();
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
    tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf")));
    tableDesc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
    TEST_UTIL.createTable(tableDesc, null);
    conf.setBoolean("hbase.cluster.managed", false);
    conf.setBoolean("hbase.client.metrics.report", false);
//    conf.set("hbase.client.keytab.file", KEYTAB_FILE.getAbsolutePath());
//    conf.set("hbase.client.kerberos.principal", hbasePrincipal);
    conf.set("hbase.client.authentication", "token");
    ConfUtil.setCustomConf(conf);
    client = HBaseClientImpl.getInstance();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (KDC != null) {
      KDC.stop();
    }
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testSecureOperation() throws Throwable {
    Put put = new Put("test001");
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("24"));
    client.put(tableName, put);
    LOG.info("=====================put a record=====================");
    Thread.sleep(330000);
    put = new Put("test002");
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("24"));
    client.put(tableName, put);
    LOG.info("=====================put a record=====================");
  }

  private static MiniKdc setupMiniKdc() throws Exception {
    Properties conf = MiniKdc.createConf();
    conf.put(MiniKdc.DEBUG, true);
    conf.setProperty(MiniKdc.MAX_TICKET_LIFETIME, "300000");
    conf.setProperty(MiniKdc.MAX_RENEWABLE_LIFETIME, "320000");
    MiniKdc kdc = null;
    File dir = null;
    boolean bindException;
    int numTries = 0;
    do {
      try {
        bindException = false;
        dir = new File(TEST_UTIL.getDataTestDir("kdc").toUri().getPath());
        kdc = new MiniKdc(conf, dir);
        kdc.start();
      } catch (BindException e) {
        FileUtils.deleteDirectory(dir); // clean directory
        numTries++;
        if (numTries == 3) {
          LOG.error("Failed setting up MiniKDC. Tried " + numTries + " times.");
          throw e;
        }
        LOG.error("BindException encountered when setting up MiniKdc. Trying again.");
        bindException = true;
      }
    } while (bindException);
    return kdc;
  }
}
