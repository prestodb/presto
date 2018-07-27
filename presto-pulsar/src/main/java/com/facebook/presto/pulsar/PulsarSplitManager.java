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
package com.facebook.presto.pulsar;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.bookkeeper.conf.ClientConfiguration;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PulsarSplitManager implements ConnectorSplitManager {

    private final String connectorId;

    private final PulsarConnectorConfig pulsarConnectorConfig;

    private final PulsarAdmin pulsarAdmin;

    private static final Logger log = Logger.get(PulsarSplitManager.class);

    @Inject
    public PulsarSplitManager(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pulsarConnectorConfig = requireNonNull(pulsarConnectorConfig, "pulsarConnectorConfig is null");
        try {
            this.pulsarAdmin = pulsarConnectorConfig.getPulsarAdmin();
        } catch (PulsarClientException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                          ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy) {

        int numSplits = this.pulsarConnectorConfig.getTargetNumSplits();

        PulsarTableLayoutHandle layoutHandle = (PulsarTableLayoutHandle) layout;
        PulsarTableHandle tableHandle = layoutHandle.getTable();

        TopicName topicName = TopicName.get("persistent", NamespaceName.get(tableHandle.getSchemaName()),
 tableHandle.getTableName());

        SchemaInfo schemaInfo;
        try {
            schemaInfo = this.pulsarAdmin.schemas().getSchemaInfo(
                    String.format("%s/%s", tableHandle.getSchemaName(), tableHandle.getTableName()));
        } catch (PulsarAdminException e) {
            log.error(e);
            throw new RuntimeException(e);
        }

        Collection<PulsarSplit> splits;
        try {
            if (!PulsarConnectorUtils.isPartitionedTopic(topicName, this.pulsarAdmin)) {
                splits = getSplitsNonPartitionedTopic(numSplits, topicName, tableHandle, schemaInfo);
            } else {
                splits = getSplitsPartitionedTopic(numSplits, topicName, tableHandle, schemaInfo);
            }
        } catch (Exception e) {
            log.error(e, "Failed to get splits");
           throw new RuntimeException(e);
        }

        return new FixedSplitSource(splits);
    }

    @VisibleForTesting
    ManagedLedgerFactory getManagedLedgerFactory() throws Exception {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration()
                .setZkServers(this.pulsarConnectorConfig.getZookeeperUri())
                .setAllowShadedLedgerManagerFactoryClass(true)
                .setShadedLedgerManagerFactoryClassPrefix("org.apache.pulsar.shade.");
        return new ManagedLedgerFactoryImpl(bkClientConfiguration);
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsPartitionedTopic(int numSplits, TopicName topicName, PulsarTableHandle
            tableHandle, SchemaInfo schemaInfo) throws Exception {
        int numPartitions;
        try {
            numPartitions = (this.pulsarAdmin.topics().getPartitionedTopicMetadata(topicName.toString())).partitions;
        } catch (PulsarAdminException e) {
            log.error(e);
            throw new RuntimeException(e);
        }

        int actualNumSplits = Math.max(numPartitions, numSplits);

        int splitsPerPartition = actualNumSplits / numPartitions;

        int splitRemainder = actualNumSplits % numPartitions;

        ManagedLedgerFactory managedLedgerFactory = getManagedLedgerFactory();

        try {
            List<PulsarSplit> splits = new LinkedList<>();
            for (int i = 0; i < numPartitions; i++) {

                int splitsForThisPartition = (splitRemainder > i) ? splitsPerPartition + 1 : splitsPerPartition;
                splits.addAll(
                        getSplitsForTopic(
                                topicName.getPartition(i).getPersistenceNamingEncoding(),
                                managedLedgerFactory,
                                splitsForThisPartition,
                                tableHandle,
                                schemaInfo,
                                topicName.getPartition(i).getLocalName())
                );
            }
            return splits;
        } finally {
            if (managedLedgerFactory != null) {
                try {
                    managedLedgerFactory.shutdown();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsForTopic(String topicNamePersistenceEncoding,
                                                      ManagedLedgerFactory managedLedgerFactory,
                                                      int numSplits,
                                                      PulsarTableHandle tableHandle,
                                                      SchemaInfo schemaInfo, String tableName)
            throws ManagedLedgerException, InterruptedException {

        ReadOnlyCursor readOnlyCursor = null;
        try {
            readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                    topicNamePersistenceEncoding,
                    PositionImpl.earliest, new ManagedLedgerConfig());
            long numEntries = readOnlyCursor.getNumberOfEntries();
            log.info("numEntries: %s", numEntries);
            if (numEntries <= 0) {
                return Collections.EMPTY_LIST;
            }
            long remainder = numEntries % numSplits;

            long avgEntriesPerSplit = numEntries / numSplits;

            List<PulsarSplit> splits = new LinkedList<>();
            for (int i = 0; i < numSplits; i++) {
                long entriesForSplit = (remainder > i) ? avgEntriesPerSplit + 1 : avgEntriesPerSplit;
                log.info("i: %s entriesPerSplit: %s", i, entriesForSplit);
                PositionImpl startPosition = (PositionImpl) readOnlyCursor.getReadPosition();
                log.info("startPosition: %s", startPosition);
                readOnlyCursor.skipEntries(Math.toIntExact(entriesForSplit));
                PositionImpl endPosition = (PositionImpl) readOnlyCursor.getReadPosition();
                log.info("endPosition: %s", endPosition);

                splits.add(new PulsarSplit(i, this.connectorId,
                        tableHandle.getSchemaName(),
                        tableName,
                        entriesForSplit,
                        new String(schemaInfo.getSchema()),
                        schemaInfo.getType(),
                        startPosition.getEntryId(),
                        endPosition.getEntryId(),
                        startPosition.getLedgerId(),
                        endPosition.getLedgerId()));
            }
            return splits;
        } finally {
            if (readOnlyCursor != null) {
                try {
                    readOnlyCursor.close();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsNonPartitionedTopic(int numSplits, TopicName topicName, PulsarTableHandle
            tableHandle, SchemaInfo schemaInfo) throws Exception {
        ManagedLedgerFactory managedLedgerFactory = null;
        try {
            managedLedgerFactory = getManagedLedgerFactory();

            return getSplitsForTopic(
                    topicName.getPersistenceNamingEncoding(),
                    managedLedgerFactory,
                    numSplits,
                    tableHandle,
                    schemaInfo,
                    tableHandle.getTableName());
        } finally {
            if (managedLedgerFactory != null) {
                try {
                    managedLedgerFactory.shutdown();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }
}
