package org.chen.hbase.util;

public class OperationNotSupportedException extends RuntimeException {

  private static final long serialVersionUID = 5338008543943777268L;

  public OperationNotSupportedException(String msg) {
    super(msg);
  }
}
