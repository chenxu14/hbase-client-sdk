package org.chen.janusgraph.client;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.chen.hbase.client.MyConnectionMask;
import org.janusgraph.diskstorage.hbase.ConnectionMask;
import org.janusgraph.diskstorage.hbase.HBaseCompat;

public class MyHBaseCompat implements HBaseCompat {

  @Override
  public void setCompression(HColumnDescriptor cd, String algorithm) {
    cd.setCompressionType(Compression.Algorithm.valueOf(algorithm));
  }

  @Override
  public HTableDescriptor newTableDescriptor(String tableName) {
    TableName tn = TableName.valueOf(tableName);
    return new HTableDescriptor(tn);
  }

  @Override
  public ConnectionMask createConnection(Configuration conf) throws IOException {
    return new MyConnectionMask();
  }

  @Override
  public void addColumnFamilyToTableDescriptor(HTableDescriptor tableDescriptor,
      HColumnDescriptor columnDescriptor) {
    tableDescriptor.addFamily(columnDescriptor);
  }

  @Override
  public void setTimestamp(Delete d, long timestamp) {
    d.setTimestamp(timestamp);
  }

}
