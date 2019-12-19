package org.chen.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.chen.hbase.util.MyConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.chen.hbase.Put;

public class TestHBaseClient {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HBaseClientImpl client;
  private static final String tableName = "testtable";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
    tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf")));
    tableDesc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
    TEST_UTIL.createTable(tableDesc, null);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(MyConstants.CLUSTER_MANAGED, false);
    ConfUtil.setCustomConf(conf);
    client = HBaseClientImpl.getInstance();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Test
  public void testOperation() throws Throwable{
    assertTrue(HBaseClientImpl.isInstanced());
    Put put = new Put("test001");
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("24"));
    client.put(tableName, put);
    assertEquals("zhangsan", client.get(tableName, "test001", "cf", "name"));
    assertEquals("24", client.get(tableName, "test001", "cf", "age"));
    assertNull(client.get(tableName, "test002", "cf", "age"));
    
    put = new Put("test002");
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("25"));
    client.put(tableName, put);

    List<Pair<String,String>> cols = new ArrayList<Pair<String,String>>();
    cols.add(new Pair<String,String>("name","xiaoliu"));
    cols.add(new Pair<String,String>("age","26"));
    client.put(tableName, "cf", "test003", cols);

    client.scan(tableName, new Scan(), new ResultCallback() {
      @Override
      public void callWithScanner(ResultScanner scanner) throws IOException {
        int i = 0;
        for(Iterator<Result> it = scanner.iterator(); it.hasNext();) {
          Result res = it.next();
          for(Cell cell : res.listCells()){
            String column = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
            LOG.info("column:" + column + ", value: " + value);
            i++;
          }
        }
        assertEquals(6, i);
      }
    });

    AggregationClient aggClient = client.getAggregationClient();
    long num = aggClient.rowCount(tableName, new LongColumnInterpreter(), new Scan());
    LOG.info("row num :" + num);
    assertEquals(3, num);
  }
}
