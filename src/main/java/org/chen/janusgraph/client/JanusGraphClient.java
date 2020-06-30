package org.chen.janusgraph.client;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class JanusGraphClient {
  private static final Log LOG = LogFactory.getLog(JanusGraphClient.class);
  private static Map<String, JanusGraph> instances = new ConcurrentHashMap<String, JanusGraph>();
  private static Properties globalProps = new Properties();

  public static JanusGraph getInstance(String table) {
    return getInstance(table, globalProps);
  }

  public static JanusGraph getInstance(String table, Properties props) {
    if (!instances.containsKey(table) || instances.get(table).isClosed()) {
      synchronized (JanusGraphClient.class) {
        if (!instances.containsKey(table) || instances.get(table).isClosed()) {
          LOG.info("create new JanusGraph instance for " + table);
          if (props == null) {
            props = globalProps;
          }
          JanusGraphFactory.Builder builder = JanusGraphFactory.build();
          props.forEach((k, v) -> builder.set((String)k, v));
          JanusGraph graph = builder.set("storage.backend", "hbase")
              .set("storage.hbase.compat-class", "org.chen.janusgraph.client.MyHBaseCompat")
              .set("storage.hbase.table", table)
              .set("query.force-index", props.getOrDefault("query.force-index", true))
              .set("graph.set-vertex-id", props.getOrDefault("graph.set-vertex-id", false))
              .set("schema.default", props.getOrDefault("schema.default", "none"))
              .set("storage.hbase.skip-schema-check", true)
              .set("metrics.enabled", props.getOrDefault("metrics.enabled", false))
              .set("storage.batch-loading", props.getOrDefault("storage.batch-loading", false))
              .open();
          instances.put(table, graph);
        }
      }
    }
    return instances.get(table);
  }
}
