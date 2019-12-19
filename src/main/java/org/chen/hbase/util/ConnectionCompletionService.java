package org.chen.hbase.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

public class ConnectionCompletionService<V> {
  private final ConnectionExecutorService executor;
  private final BlockingQueue<Future<V>> completionQueue;

  private class QueueingFuture extends FutureTask<Void> {
    private final Future<V> task;
    QueueingFuture(RunnableFuture<V> task) {
      super(task, null);
      this.task = task;
    }
    protected void done() {
      completionQueue.add(task);
    }
  }

  public ConnectionCompletionService(ConnectionExecutorService executor) {
    this.executor = executor;
    this.completionQueue = new LinkedBlockingQueue<Future<V>>();
  }

  public Future<V> submit(ConnectionCallable<V> task) {
    RunnableFuture<V> f = executor.newTaskFor(task);
    executor.execute(new QueueingFuture(f), task.isPrimary());
    return f;
  }

  public Future<V> take() throws InterruptedException {
    return completionQueue.take();
  }

  public Future<V> poll() {
    return completionQueue.poll();
  }

  public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
    return completionQueue.poll(timeout, unit);
  }
}
