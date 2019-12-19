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

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface HashingStrategy {
  int hashPrefix(String rowkey);

  HashingStrategy DJB_HASHER = new HashingStrategy() {
    @Override
    public int hashPrefix(String rowkey) {
      int hash = 5381;
      for (int i = 0; i < rowkey.length(); i++) {
        hash = ((hash << 5) + hash) + rowkey.charAt(i);
      }
      return (hash & 0x7FFFFFFF);
    }
  };
}
