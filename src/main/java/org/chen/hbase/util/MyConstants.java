package org.chen.hbase.util;

/**
 * some Constants used by other class
 */
public final class MyConstants {
  /**
   * if the cluster is managed(set zkQurom Unnecessary)
   */
  public static final String CLUSTER_MANAGED = "hbase.cluster.managed";
  public static final boolean CLUSTER_MANAGED_DEFAULT = true;
  /**
   * when connection setup, these table instance will create and cached
   */
  public static final String CLIENT_HOTTABLE = "hbase.client.hottable";
  public static final String CLIENT_WATCH_TABLE = "hbase.client.watch.table";
  public static final String CLIENT_AUTO_FLUSH = "hbase.client.autoflush";

  public static final String CLIENT_AUTH_PERIOD = "hbase.client.auth.refresh";
  public static final int CLIENT_AUTH_DEFAULT = 3600000;

  public static final String CLIENT_PARSCAN_NUM = "hbase.client.parscan.threadnums";

  public static final String SALT_DELIMITER = "_"; 
  /**
   * if managed, krb5.conf will download to this file
   */
  public static final String KRB5_CONF_PATH = "hbase.client.krb5conf.path";
  public static final String KRB5_CONF_DEFAULT = "${user.dir}";
  public static final String UNMANAGED_MODEL = "unmanaged";
  public static final String DEFAULT_PRINCIPAL = "hbase/_HOST@REALM.COM";
  public static final String TABLE_PREFIX = "table_";
  public static final String CLUSTER_PREFIX = "cluster_";
  public static final String APPKEY = "org.chen.service.hbase.hbaseadmin";
}
