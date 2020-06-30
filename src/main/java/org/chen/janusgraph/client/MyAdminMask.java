package org.chen.janusgraph.client;

import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.janusgraph.diskstorage.hbase.AdminMask;

/**
 * currently nothing to do, because we set storage.hbase.skip-schema-check to true
 */
public class MyAdminMask implements AdminMask {

  @Override
  public void close() throws IOException {

  }

  @Override
  public void clearTable(String tableName, long timestamp) throws IOException {

  }

  @Override
  public void dropTable(String tableName) throws IOException {

  }

  @Override
  public HTableDescriptor getTableDescriptor(String tableName) throws IOException {
    return null;
  }

  @Override
  public boolean tableExists(String tableName) throws IOException {
    return false;
  }

  @Override
  public void createTable(HTableDescriptor desc) throws IOException {

  }

  @Override
  public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions) throws IOException {

  }

  @Override
  public int getEstimatedRegionServerCount() {
    return 0;
  }

  @Override
  public void disableTable(String tableName) throws IOException {

  }

  @Override
  public void enableTable(String tableName) throws IOException {

  }

  @Override
  public boolean isTableDisabled(String tableName) throws IOException {
    return false;
  }

  @Override
  public void addColumn(String tableName, HColumnDescriptor columnDescriptor) throws IOException {

  }

  @Override
  public void snapshot(String snapshotName, String table) throws IllegalArgumentException, IOException {

  }

  @Override
  public void deleteSnapshot(String snapshotName) throws IOException {

  }
}
