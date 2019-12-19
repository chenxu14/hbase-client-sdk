package org.chen.hbase.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ConnectionExecutorService implements ExecutorService {
  private final ExecutorService primary;
  private final Optional<ExecutorService> secondary;

  public ConnectionExecutorService(ExecutorService primary, ExecutorService secondary) {
    this.primary = primary;
    this.secondary = Optional.ofNullable(secondary);
  }

  @Override
  public void shutdown() {
    primary.shutdown();
    secondary.ifPresent(s -> s.shutdown());
  }

  @Override
  public List<Runnable> shutdownNow() {
    secondary.ifPresent(s -> s.shutdownNow());
    return primary.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return primary.isShutdown() && secondary.map(s -> s.isShutdown()).orElse(true);
  }

  @Override
  public boolean isTerminated() {
    return primary.isTerminated() && secondary.map(s -> s.isTerminated()).orElse(true);
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    boolean primaryAwait = primary.awaitTermination(timeout, unit);
    boolean secondaryAwait = true;
    if (secondary.isPresent()) {
      secondaryAwait = secondary.get().awaitTermination(timeout, unit);
    }
    return primaryAwait && secondaryAwait;
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    if (!(task instanceof ConnectionCallable)) {
      throw new OperationNotSupportedException("task should be an instance of ClientCallable");
    }
    ConnectionCallable<T> callable = (ConnectionCallable<T>) task;
    if (callable.isPrimary()) {
      return this.primary.submit(callable);
    } else {
      return this.secondary.map(s->s.submit(callable)).orElse(null);
    }
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    throw new OperationNotSupportedException("submit operation not supported yet");
  }

  @Override
  public Future<?> submit(Runnable task) {
    throw new OperationNotSupportedException("submit operation not supported yet");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    if (tasks == null) {
      throw new NullPointerException();
    }
    ArrayList<Future<T>> futures = new ArrayList<Future<T>>(tasks.size());
    boolean done = false;
    try {
      for (Callable<T> t : tasks) {
        RunnableFuture<T> f = newTaskFor(t);
        futures.add(f);
        execute(f, ((ConnectionCallable<T>)t).isPrimary());
      }
      for (int i = 0, size = futures.size(); i < size; i++) {
        Future<T> f = futures.get(i);
        if (!f.isDone()) {
          try {
            f.get();
          } catch (CancellationException ignore) {
          } catch (ExecutionException ignore) {
          }
        }
      }
      done = true;
      return futures;
    } finally {
      if (!done) {
        for (int i = 0, size = futures.size(); i < size; i++) {
          futures.get(i).cancel(true);
        }
      }
    }
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    if (tasks == null) {
      throw new NullPointerException();
    }
    long nanos = unit.toNanos(timeout);
    ArrayList<Future<T>> futures = new ArrayList<Future<T>>(tasks.size());
    final long deadline = System.nanoTime() + nanos;
    boolean done = false;
    try {
      for (Callable<T> t : tasks) {
        RunnableFuture<T> f = newTaskFor(t);
        futures.add(f);
        execute(f, ((ConnectionCallable<T>)t).isPrimary());
        nanos = deadline - System.nanoTime();
        if (nanos <= 0L) {
          return futures;
        }
      }
      final int size = futures.size();
      for (int i = 0; i < size; i++) {
        Future<T> f = futures.get(i);
        if (!f.isDone()) {
          if (nanos <= 0L) {
            return futures;
          }
          try {
            f.get(nanos, TimeUnit.NANOSECONDS);
          } catch (CancellationException ignore) {
          } catch (ExecutionException ignore) {
          } catch (TimeoutException toe) {
            return futures;
          }
          nanos = deadline - System.nanoTime();
        }
      }
      done = true;
      return futures;
    } finally {
      if (!done) {
        for (int i = 0, size = futures.size(); i < size; i++) {
          futures.get(i).cancel(true);
        }
      }
    }
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    try {
      return doInvokeAny(tasks, false, 0);
    } catch (TimeoutException cannotHappen) {
      assert false;
      return null;
    }
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doInvokeAny(tasks, true, unit.toNanos(timeout));
  }

  protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    return new FutureTask<T>(runnable, value);
  }

  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    return new FutureTask<T>(callable);
  }

  @Override
  public void execute(Runnable command) {
    throw new OperationNotSupportedException("execute operation not supported yet");
  }

  public void execute(Runnable command, boolean isPrimary) {
    if (isPrimary) {
      this.primary.execute(command);
    } else {
      secondary.ifPresent(s -> s.execute(command));
    }
  }

  private <T> T doInvokeAny(Collection<? extends Callable<T>> tasks, boolean timed, long nanos)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (tasks == null) {
      throw new NullPointerException();
    }
    int ntasks = tasks.size();
    if (ntasks == 0) {
      throw new IllegalArgumentException();
    }
    ArrayList<Future<T>> futures = new ArrayList<Future<T>>(ntasks);
    ConnectionCompletionService<T> ecs = new ConnectionCompletionService<T>(this);
    try {
      ExecutionException ee = null;
      final long deadline = timed ? System.nanoTime() + nanos : 0L;
      Iterator<? extends Callable<T>> it = tasks.iterator();
      futures.add(ecs.submit((ConnectionCallable<T>)it.next()));
      --ntasks;
      int active = 1;
      for (;;) {
        Future<T> f = ecs.poll();
        if (f == null) {
          if (ntasks > 0) {
            --ntasks;
            futures.add(ecs.submit((ConnectionCallable<T>)it.next()));
            ++active;
          } else if (active == 0) {
            break;
          } else if (timed) {
            f = ecs.poll(nanos, TimeUnit.NANOSECONDS);
            if (f == null)
              throw new TimeoutException();
            nanos = deadline - System.nanoTime();
          } else {
            f = ecs.take();
          }
        }
        if (f != null) {
          --active;
          try {
            return f.get();
          } catch (ExecutionException eex) {
            ee = eex;
          } catch (RuntimeException rex) {
            ee = new ExecutionException(rex);
          }
        }
      }
      if (ee == null) {
        ee = new ExecutionException("doInvokeAny failed", null);
      }
      throw ee;
    } finally {
      for (int i = 0, size = futures.size(); i < size; i++) {
        futures.get(i).cancel(false);
        // futures.get(i).cancel(true);
      }
    }
  }
}
