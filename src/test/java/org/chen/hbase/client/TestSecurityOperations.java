package org.chen.hbase.client;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.chen.hbase.Put;

@Category(SmallTests.class)
public class TestSecurityOperations {
  final Log LOG = LogFactory.getLog(getClass());
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final File KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri().getPath());
  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String hbasePrincipal;
  private static String clientPrincipal;
  private static String spnegoPrincipal;
  private static HBaseClient client;
  private static final String tableName = "testtable";

  @BeforeClass
  public static void setUp() throws Exception {
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    KDC.createPrincipal(KEYTAB_FILE, "hbase/" + HOST, "hbase-client/" + HOST, "HTTP/" + HOST);
    hbasePrincipal = "hbase/" + HOST + "@" + KDC.getRealm();
    clientPrincipal = "hbase-client/" + HOST + "@" + KDC.getRealm();
    spnegoPrincipal = "HTTP/" + HOST + "@" + KDC.getRealm();
    Configuration conf = TEST_UTIL.getConfiguration();
    // hbase security conf
    conf.set("hbase.master.keytab.file", KEYTAB_FILE.getAbsolutePath());
    conf.set("hbase.master.kerberos.principal", hbasePrincipal);
    conf.set("hbase.regionserver.keytab.file", KEYTAB_FILE.getAbsolutePath());
    conf.set("hbase.regionserver.kerberos.principal", hbasePrincipal);
    conf.set("hbase.security.authentication", "kerberos");
    conf.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
    conf.setBoolean("hbase.security.authorization", true);
    conf.setBoolean("hbase.security.exec.permission.checks", true);
    // hadoop security conf
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hbasePrincipal);
    conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, KEYTAB_FILE.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hbasePrincipal);
    conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, KEYTAB_FILE.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setBoolean(DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY, true);
    conf.set(DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    TEST_UTIL.startMiniCluster();
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
    tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf")));
    tableDesc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
    TEST_UTIL.createTable(tableDesc, null);
    conf.setBoolean("hbase.cluster.managed", false);
    conf.setBoolean("hbase.client.metrics.report", false);
    conf.set("hbase.client.keytab.file", KEYTAB_FILE.getAbsolutePath());
    conf.set("hbase.client.kerberos.principal", clientPrincipal);
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
    
    assertEquals("zhangsan", client.get(tableName, "test001", "cf", "name"));
    assertEquals("24", client.get(tableName, "test001", "cf", "age"));

    put = new Put("test002");
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("25"));
    client.put(tableName, put);

    List<Pair<String, String>> cols = new ArrayList<Pair<String, String>>();
    cols.add(new Pair<String, String>("name", "xiaoliu"));
    cols.add(new Pair<String, String>("age", "26"));
    client.put(tableName, "cf", "test003", cols);

    client.scan(tableName, new Scan(), (scanner) -> {
      int i = 0;
      for (Iterator<Result> it = scanner.iterator(); it.hasNext();) {
        Result res = it.next();
        for (Cell cell : res.listCells()) {
          String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
              cell.getQualifierLength());
          String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
          LOG.info("column:" + column + ", value: " + value);
          i++;
        }
      }
      assertEquals(6, i);
    });

    AggregationClient aggClient = client.getAggregationClient();
    long num = aggClient.rowCount(tableName, new LongColumnInterpreter(), new Scan());
    LOG.info("row num :" + num);
    assertEquals(3, num);
  }
}
