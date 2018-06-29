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
import io.airlift.log.Logger;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.bookkeeper.conf.ClientConfiguration;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PulsarSplitManager implements ConnectorSplitManager {

    private final String connectorId;

    private final PulsarConnectorConfig pulsarConnectorConfig;

    private static final Logger log = Logger.get(PulsarSplitManager.class);


    @Inject
    public PulsarSplitManager(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pulsarConnectorConfig = requireNonNull(pulsarConnectorConfig, "pulsarConnectorConfig is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                          ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy) {

        int numSplits = 3;

        PulsarTableLayoutHandle layoutHandle = (PulsarTableLayoutHandle) layout;
        PulsarTableHandle tableHandle = layoutHandle.getTable();

        TopicName topicName = TopicName.get("persistent", NamespaceName.get(tableHandle.getSchemaName()),
 tableHandle.getTableName());

        SchemaInfo schemaInfo;
        try {
            schemaInfo = pulsarConnectorConfig.getPulsarAdmin().schemas().getSchemaInfo(
                    String.format("%s/%s", tableHandle.getSchemaName(), tableHandle.getTableName()));
        } catch (PulsarAdminException | PulsarClientException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
        String schemaJson = new String(schemaInfo.getSchema());

        Collection<PulsarSplit> splits;
        try {
           splits = getSplits(numSplits, topicName, tableHandle, schemaJson);
        } catch (Exception e) {
            log.error(e, "Failed to get splits");
           throw new RuntimeException(e);
        }

        return new FixedSplitSource(splits);
    }

    private Collection<PulsarSplit> getSplits(int numSplits, TopicName topicName, PulsarTableHandle
            tableHandle, String schemaJson) throws Exception {
        List<PulsarSplit> splits = new ArrayList<>(numSplits);
        ManagedLedgerFactory managedLedgerFactory = null;
        ReadOnlyCursor readOnlyCursor = null;
        try {
            ClientConfiguration bkClientConfiguration = new ClientConfiguration()
                    .setZkServers(pulsarConnectorConfig.getZookeeperUri())
                    .setAllowShadedLedgerManagerFactoryClass(true)
                    .setShadedLedgerManagerFactoryClassPrefix("org.apache.pulsar.shade.");
            managedLedgerFactory = new ManagedLedgerFactoryImpl(bkClientConfiguration);
            ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize
                    (1).setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
                    .setMetadataAckQuorumSize(1);

            readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(topicName.getPersistenceNamingEncoding(),
                    PositionImpl.earliest, managedLedgerConfig);

            long numEntries = readOnlyCursor.getNumberOfEntries();
            log.info("numEntries: %s", numEntries);
            long remainder = numEntries % numSplits;

            long avgEntriesPerSplit = numEntries / numSplits;

            for (int i = 0; i < numSplits; i++) {
                long entriesForSplit = (remainder > i) ? avgEntriesPerSplit + 1 : avgEntriesPerSplit;
                log.info("i: %s entriesPerSplit: %s", i, entriesForSplit);
                PositionImpl startPosition = (PositionImpl) readOnlyCursor.getReadPosition();
                log.info("startPosition: %s", startPosition);
                readOnlyCursor.skipEntries(Math.toIntExact(entriesForSplit));
                PositionImpl endPosition = (PositionImpl) readOnlyCursor.getReadPosition();
                log.info("endPosition: %s", endPosition);

                splits.add(new PulsarSplit(i, this.connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(),
                        entriesForSplit, schemaJson, startPosition.getEntryId(),
                        endPosition.getEntryId(), startPosition.getLedgerId(), endPosition.getLedgerId()));
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
