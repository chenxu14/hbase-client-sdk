package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.chen.hbase.MTable;

public class RpcRetryingCallerWithClusterReplicas {
  private final CompositeConnection conn;
  private final Get get;
  private final MTable table;
  private final int retries;
  private final int operationTimeout;
  private final int rpcTimeout;
  private final RpcControllerFactory rpcControllerFactory;

  public RpcRetryingCallerWithClusterReplicas(CompositeConnection conn, MTable table, Get get) {
    this.get = get;
    this.table = table;
    this.conn = conn;
    this.retries = conn.getConnectionConfiguration().getRetriesNumber();
    this.operationTimeout = conn.getConnectionConfiguration().getOperationTimeout();
    this.rpcTimeout = conn.getConnectionConfiguration().getReadRpcTimeout();
    this.rpcControllerFactory = conn.getRpcControllerFactory();
  }

  public synchronized Result call() throws DoNotRetryIOException, InterruptedIOException, RetriesExhaustedException {
    ResultBoundedCompletionService<Result> cs = new ResultBoundedCompletionService<Result>(
        conn.getRpcRetryingCallerFactory(), conn.getThreadPool(),
        conn.getBackupConnection().isPresent() ? 2 : 1);
    addCallsForReplica((ClusterConnection)conn.getActiveConnection(), cs, 0);
    if (conn.getBackupConnection().isPresent()) {
      addCallsForReplica((ClusterConnection)conn.getBackupConnection().get(), cs, 1);
    }
    try {
      try {
        Future<Result> f = cs.take();
        return f.get();
      } catch (ExecutionException e) {
        RpcRetryingCallerWithReadReplicas.throwEnrichedException(e, retries);
      }
    } catch (CancellationException e) {
      throw new InterruptedIOException();
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    } finally {
      // We get there because we were interrupted or because one or more of the
      // calls succeeded or failed. In all case, we stop all our tasks.
      cs.cancelAll();
    }
    return null;
  }

  private void addCallsForReplica(ClusterConnection c, ResultBoundedCompletionService<Result> cs, int id)
      throws DoNotRetryIOException, RetriesExhaustedException, InterruptedIOException {
    RegionLocations rls = RpcRetryingCallerWithReadReplicas
        .getRegionLocations(true, 0, c, table.getName(), get.getRow());
    ReplicaClusterCallable callable = new ReplicaClusterCallable(c, rls.getRegionLocation(0));
    cs.submit(callable, this.operationTimeout, id);
  }

  class ReplicaClusterCallable extends CancellableRegionServerCallable<Result> {
    public ReplicaClusterCallable(Connection c, HRegionLocation location) {
      super(c, table.getName(), get.getRow(), rpcControllerFactory.newController(),
          rpcTimeout, new RetryingTimeTracker());
	  this.location = location;
	}

	@Override
	public void prepare(boolean reload) throws IOException {
      if (getRpcController().isCanceled()) {
        return;
      }
      if (Thread.interrupted()) {
        throw new InterruptedIOException();
      }
      if (reload || location == null) {
        RegionLocations rl = RpcRetryingCallerWithReadReplicas.getRegionLocations(
            false, 0, getConnection(), table.getName(), get.getRow());
        location = rl.getRegionLocation(0);
      }
      if (location == null || location.getServerName() == null) {
        // With this exception, there will be a retry. The location can be null for a replica
        //  when the table is created or after a split.
        throw new HBaseIOException("There is no location for row " + Bytes.toString(get.getRow()));
      }
      setStubByServiceName(this.location.getServerName());
	}

	@Override
	protected Result rpcCall() throws Exception {
      if (getRpcController().isCanceled()) {
        return null;
      }
      if (Thread.interrupted()) {
        throw new InterruptedIOException();
      }
      byte[] reg = location.getRegionInfo().getRegionName();
      ClientProtos.GetRequest request = RequestConverter.buildGetRequest(reg, get);
      // Presumption that we are passed a PayloadCarryingRpcController here!
      PayloadCarryingRpcController hrc = (PayloadCarryingRpcController) getRpcController();
      hrc.reset();
      hrc.setCallTimeout(rpcTimeout);
      hrc.setPriority(table.getName());
      ClientProtos.GetResponse response = getStub().get(hrc, request);
      if (response == null) {
        return null;
      }
      return ProtobufUtil.toResult(response.getResult(), hrc.cellScanner());
	}
  }
}
