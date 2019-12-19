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
 * Used to perform Delete operations on a single row.
 * <p>
 * To delete an entire row, instantiate a Delete object with the row
 * to delete.  To further define the scope of what to delete, perform
 * additional methods as outlined below.
 * <p>
 * To delete specific families, execute {@link #addFamily(byte[]) deleteFamily}
 * for each family to delete.
 * <p>
 * To delete multiple versions of specific columns, execute
 * {@link #addColumns(byte[], byte[]) deleteColumns}
 * for each column to delete.
 * <p>
 * To delete specific versions of specific columns, execute
 * {@link #addColumn(byte[], byte[], long) deleteColumn}
 * for each column version to delete.
 * <p>
 * Specifying timestamps, deleteFamily and deleteColumns will delete all
 * versions with a timestamp less than or equal to that passed.  If no
 * timestamp is specified, an entry is added with a timestamp of 'now'
 * where 'now' is the servers's System.currentTimeMillis().
 * Specifying a timestamp to the deleteColumn method will
 * delete versions only with a timestamp equal to that specified.
 * If no timestamp is passed to deleteColumn, internally, it figures the
 * most recent cell's timestamp and adds a delete at that timestamp; i.e.
 * it deletes the most recently added cell.
 * <p>The timestamp passed to the constructor is used ONLY for delete of
 * rows.  For anything less -- a deleteColumn, deleteColumns or
 * deleteFamily -- then you need to use the method overrides that take a
 * timestamp.  The constructor timestamp is not referenced.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Delete extends org.apache.hadoop.hbase.client.Delete {
  public Delete(String rowkey) {
    this(rowkey, System.currentTimeMillis(), HashingStrategy.DJB_HASHER);
  }

  public Delete(String rowkey, long ts) {
    this(rowkey, ts, HashingStrategy.DJB_HASHER);
  }

  public Delete(String rowkey, HashingStrategy strategy) {
    this(rowkey, System.currentTimeMillis(), strategy);
  }

  public Delete(String rowkey, long ts, HashingStrategy strategy) {
    super(Bytes.toBytes(new StringBuilder(String.format("%04d", strategy.hashPrefix(rowkey) % 10000))
        .append("_").append(rowkey).toString()), ts, true);
  }
}
