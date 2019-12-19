package org.chen.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

public class TestConfUtil {
  @Test
  public void testParseClusterName() {
    String json = "[{\"clusterName\":\"hbase-nh\",\"backupCluster\":\"hbase-xr\",\"state\":\"RUNNING\",\"realName\":\"test:TestTable\"}]";
    try {
      TableInfo tableInfo = ConfUtil.parseTableInfo("test_TestTable", json);
      assertEquals(tableInfo.getClusterName(), "hbase-nh");
      assertEquals(tableInfo.getBackupCluster(), "hbase-xr");
      assertEquals(tableInfo.getRealName(), "test:TestTable");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testParseZKQurom(){
    String json = "{\"name\":\"hbase-nh\",\"state\":\"RUNNING\",\"zkServers\":\"hbase02.nh,hbase03.nh,hbase04.nh\",\"zkPort\":\"2181\"}";
    try {
      ClusterInfo clusterInfo = ConfUtil.parseClusterInfo("hbase-nh", json);
      assertEquals(clusterInfo.getZkServers(),"hbase02.nh,hbase03.nh,hbase04.nh");
      assertEquals(clusterInfo.getZkPort(),"2181");
      assertEquals(clusterInfo.getName(),"hbase-nh");
      assertEquals(clusterInfo.getState(),"RUNNING");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
