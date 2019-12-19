package org.chen.hbase.util;

public interface IConfigChangeListener {
  void changed(String key, String oldValue, String newValue);
}
