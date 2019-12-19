package org.chen.hbase.client;

public class TableInfo {
  private String clusterName;
  private String backupCluster;
  private String realName;
  private int partitions;
  private boolean failover;

  public TableInfo() { }

  public TableInfo(String clusterName, String backupCluster, String realName,
      int partitions, boolean failover) {
    this.clusterName = clusterName;
    this.backupCluster = backupCluster;
    this.realName = realName;
    this.partitions = partitions;
    this.failover = failover;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getBackupCluster() {
    return backupCluster;
  }

  public void setBackupCluster(String backupCluster) {
    this.backupCluster = backupCluster;
  }

  public String getRealName() {
    return realName;
  }

  public void setRealName(String realName) {
    this.realName = realName;
  }

  public int getPartitions() {
    return partitions;
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public boolean isFailover() {
    return failover;
  }

  public void setFailover(boolean failover) {
    this.failover = failover;
  }
}
