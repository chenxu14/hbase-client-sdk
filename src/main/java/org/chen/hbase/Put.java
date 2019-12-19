/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.chen.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Used to perform Put operations for a single row.
 * <p>
 * To perform a Put, instantiate a Put object with the row to insert to and
 * for eachumn to be inserted, execute {@link #add(byte[], byte[], byte[]) add} or
 * {@link #add(byte[], byte[], long, byte[]) add} if setting the timestamp.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Put extends org.apache.hadoop.hbase.client.Put {
  public Put(String rowkey) {
    // can't use HConstants#LATEST_TIMESTAMP now, because we writer KAFKA first
    // it must have a timestamp, or Get with column specify will return nothing
    // because it was skiped by ExplicitColumnTracker#checkVersions
    this(rowkey, System.currentTimeMillis(), HashingStrategy.DJB_HASHER);
  }

  /**
   * Create a Put operation for the specified row, using a given timestamp.
   */
  public Put(String rowkey, long ts) {
    this(rowkey, ts, HashingStrategy.DJB_HASHER);
  }

  /**
   * Create a Put operation for the specified row, using a given hash strategy.
   */
  public Put(String rowkey, HashingStrategy strategy) {
    this(rowkey, System.currentTimeMillis(), strategy);
  }

  public Put(String rowkey, long ts, HashingStrategy strategy) {
    super(Bytes.toBytes(new StringBuilder(String.format("%04d", strategy.hashPrefix(rowkey) % 10000))
        .append("_").append(rowkey).toString()), ts, true);
  }
}
