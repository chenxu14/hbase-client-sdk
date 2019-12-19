package org.chen.hbase.util;

import java.util.concurrent.Callable;

public abstract class ConnectionCallable<V> implements Callable<V> {
  private final boolean primary;
  public ConnectionCallable(boolean primary) {
    this.primary = primary;
  }
  public boolean isPrimary() {
    return primary;
  }
}
