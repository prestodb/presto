/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.metastore.thrift;

import org.apache.thrift.TException;

import java.util.Optional;

/**
 * A Hive cluster is a single logical installation of Hive. It might
 * have multiple instances of the metastore service (for scalability
 * purposes), but they would all return the same data.
 * <p/>
 * This Hive plugin only supports having a single Hive cluster per
 * instantiation of the plugin, but a plugin that extends this code
 * could support multiple, dynamically located Hive clusters.
 */
public interface HiveCluster
{
    /**
     * Create a connected {@link HiveMetastoreClient} to this HiveCluster
     */
    HiveMetastoreClient createMetastoreClient(Optional<String> token)
            throws TException;
}
