/**
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

@InterfaceAudience.Public
@InterfaceStability.Stable
public class Get extends org.apache.hadoop.hbase.client.Get {
  public Get(String rowkey) {
    this(rowkey, HashingStrategy.DJB_HASHER);
  }

  public Get(String rowkey, HashingStrategy strategy) {
    super(Bytes.toBytes(new StringBuilder(String.format("%04d", strategy.hashPrefix(rowkey) % 10000))
        .append("_").append(rowkey).toString()));
  }
}
