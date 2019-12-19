package org.chen.hbase.client;

public class ClusterInfo {
  private String name;
  private String zkServers;
  private String zkPort;
  private String state;
  private String kafkaServers;

  public ClusterInfo() {}

  public ClusterInfo(String name, String zkServers, String zkPort, String state, String kafkaServers) {
    this.name = name;
    this.zkServers = zkServers;
    this.zkPort = zkPort;
    this.state = state;
    this.kafkaServers = kafkaServers;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getZkServers() {
    return zkServers;
  }

  public void setZkServers(String zkServers) {
    this.zkServers = zkServers;
  }

  public String getZkPort() {
    return zkPort;
  }

  public void setZkPort(String zkPort) {
    this.zkPort = zkPort;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getKafkaServers() {
    return kafkaServers;
  }

  public void setKafkaServers(String kafkaServers) {
    this.kafkaServers = kafkaServers;
  }
}
