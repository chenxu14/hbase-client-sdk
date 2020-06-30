package org.chen.hbase.client;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.chen.janusgraph.client.MyAdminMask;
import org.chen.janusgraph.client.MyTableMask;
import org.janusgraph.diskstorage.hbase.AdminMask;
import org.janusgraph.diskstorage.hbase.ConnectionMask;
import org.janusgraph.diskstorage.hbase.TableMask;

public class MyConnectionMask implements ConnectionMask {

  @Override
  public void close() throws IOException {
    // no need to do with close, since HBaseClientImpl self manage it
  }

  @Override
  public TableMask getTable(String name) throws IOException {
    Table table = HBaseClientImpl.getInstance().getTable(name);
    return new MyTableMask(table);
  }

  @Override
  public AdminMask getAdmin() throws IOException {
    return new MyAdminMask();
  }

  @Override
  public List<HRegionLocation> getRegionLocations(String tableName) throws IOException {
    HBaseClientImpl client = HBaseClientImpl.getInstance();
    client.getTable(tableName).close(); // make sure connection established
    TableInfo table = client.tables.get(tableName);
    return client.getConnection(table).getRegionLocator(
        TableName.valueOf(tableName)).getAllRegionLocations();
  }

}
