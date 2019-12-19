package org.chen.hbase.client;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

public interface HBaseClient extends HBaseReadClient, HBaseWriteClient, Stoppable {
  /**
   * Client used for aggregation query
   */
  public AggregationClient getAggregationClient();
  public CoprocessorRpcChannel coprocessorService(String tableName, byte[] row) throws IOException;
  public void batch(String tableName, final List<? extends Row> actions, final Object[] results) 
      throws IOException, InterruptedException;
}
