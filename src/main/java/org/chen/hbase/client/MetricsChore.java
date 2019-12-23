package org.chen.hbase.client;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Bytes;
import org.chen.hbase.Put;
import org.chen.hbase.util.MyConstants;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.stats.Snapshot;

public class MetricsChore extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(HBaseClient.class);
  private ConcurrentHashMap<String, Histogram> reqHists;
  private final MetricsRegistry registry;
  private final byte[] FAM = Bytes.toBytes("m");
  private final byte[] COL_99 = Bytes.toBytes("p99");
  private final byte[] COL_999 = Bytes.toBytes("p999");
  private final byte[] COL_9999 = Bytes.toBytes("p9999");

  public MetricsChore(Stoppable stoppable, int period, Configuration conf) throws Exception {
    super("MetricsChore", stoppable, period, period);
    this.registry = new MetricsRegistry();
    reqHists = new ConcurrentHashMap<String,Histogram>();
  }

  public void updateRpc(String tableName, long time) {
    if (!reqHists.containsKey(tableName)) {
      Histogram reqHist = registry.newHistogram(new MetricName("client", tableName, "timePerKV"), false);
      reqHists.putIfAbsent(tableName, reqHist);
    }
    reqHists.get(tableName).update(time);
  }

  @Override
  protected void chore() {
    try {
      EmbedKafkaClient slaClient = HBaseClientImpl.getInstance().getKafkaClient(MyConstants.SLA_TABLE);
      long now = System.currentTimeMillis();
      for (Map.Entry<String, Histogram> tableMetric : reqHists.entrySet()) {
        String table = tableMetric.getKey();
        Put put = new Put(table + "_" + now);
        Snapshot metricsSnapshot = tableMetric.getValue().getSnapshot();
        tableMetric.getValue().clear();
        put.addColumn(FAM, COL_99, Bytes.toBytes(metricsSnapshot.get99thPercentile()));
        put.addColumn(FAM, COL_999, Bytes.toBytes(metricsSnapshot.get999thPercentile()));
        put.addColumn(FAM, COL_9999, Bytes.toBytes(metricsSnapshot.getValue(0.9999)));
        slaClient.putAsync(MyConstants.SLA_TABLE, put);
      }
    } catch (IOException e) {
      LOG.warn("put sla metrics error.", e);
    }
  }
}
