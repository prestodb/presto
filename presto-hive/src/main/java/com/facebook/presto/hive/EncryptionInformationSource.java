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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Responsible for taking care of returning an object which implements {@link EncryptionMetadata} if the passed in
 * {@link Table} satisfies the criteria for the implementation.
 */
public interface EncryptionInformationSource
{
    /**
     * Return encryption for a partitioned table
     *
     * @param session Session object for the associated query
     * @param table Table information
     * @param requestedColumns The columns that the query is requesting access to. The implementations can use this to only return information
     *                         for the requested columns
     * @param partitions Map from partition names to the column objects
     * @return Map from the partition name to the encryption information for that respective partition. The implementation can decide to return
     *         information for selected partitions. The implementation should return <code>Optional.empty()</code> if it does not have any
     *         encryption information available for the given table and partitions, either due to the format or the table not having any encrypted
     *         data.
     */
    Optional<Map<String, EncryptionInformation>> getReadEncryptionInformation(
            ConnectorSession session,
            Table table,
            Optional<Set<HiveColumnHandle>> requestedColumns,
            Map<String, Partition> partitions);

    /**
     * Return encryption information for an unpartitioned table
     *
     * @param session Session object for the associated query
     * @param table Table information
     * @param requestedColumns The columns that the query is requesting access to. The implementations can use this to only return information
     *                         for the requested columns
     * @return Encryption information for the given table. The implementation should return <code>Optional.empty()</code> if it does not have any
     *         encryption information available for the given table, either due to the format or the table not having any encrypted data.
     */
    Optional<EncryptionInformation> getReadEncryptionInformation(
            ConnectorSession session,
            Table table,
            Optional<Set<HiveColumnHandle>> requestedColumns);

    /**
     * Return information required for writing to a table. This method is intended to be used for both partitioned and unpartitioned tables since
     * Presto isn't aware of the partitions that are being written to ahead of time.
     *
     * @param session Session object for the associated query
     * @param tableEncryptionProperties Table encryption information
     * @param dbName database name
     * @param tableName table name
     * @return Encryption information for the given table. The implementation should return <code>Optional.empty()</code> if it does not have any
     *         encryption information available for the given table, either due to the format or the table not having any encrypted data.
     */
    Optional<EncryptionInformation> getWriteEncryptionInformation(
            ConnectorSession session,
            TableEncryptionProperties tableEncryptionProperties,
            String dbName,
            String tableName);
}
