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
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.log.Logger;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.avro.Schema;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PulsarSplitManager implements ConnectorSplitManager {

    private final String connectorId;

    private final PulsarConnectorConfig pulsarConnectorConfig;

    private static final Logger log = Logger.get(PulsarSplitManager.class);


    @Inject
    public PulsarSplitManager(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pulsarConnectorConfig = requireNonNull(pulsarConnectorConfig, "pulsarConnectorConfig is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy) {

        PulsarTableLayoutHandle layoutHandle = (PulsarTableLayoutHandle) layout;
        PulsarTableHandle tableHandle = layoutHandle.getTable();

        List<ConnectorSplit> splits = new ArrayList<>();

        int numSplits = 5;
        TopicName topicName = TopicName.get("persistent", NamespaceName.get(tableHandle.getSchemaName()), tableHandle.getTableName());
        long numEntries;
        try {
            numEntries = getNumEntries(topicName, this.pulsarConnectorConfig.getZookeeperUri());
        } catch (Exception e) {
            log.error(e, "Failed to get number of entries for topic %s", topicName);
            throw new RuntimeException(e);
        }

        long remainder = numEntries % numSplits;

        long avgEntriesPerSplit = numEntries / numSplits;

        for (int i = 0; i < 1; i ++) {
            long entriesPerSplit = (remainder > i) ? avgEntriesPerSplit + 1 : avgEntriesPerSplit;

            log.info("i: %s entriesPerSplit: %s",i, entriesPerSplit);

            SchemaInfo schemaInfo = null;
            try {
                schemaInfo = pulsarConnectorConfig.getPulsarAdmin().schemas().getSchemaInfo(
                        String.format("%s/%s", tableHandle.getSchemaName(), tableHandle.getTableName()));
            } catch (PulsarAdminException | PulsarClientException e) {
                log.error(e);
                throw new RuntimeException(e);
            }

            log.info("schema: " + new String(schemaInfo.getSchema()));
            String schemaJson = new String(schemaInfo.getSchema());

            splits.add(new PulsarSplit(i, this.connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), entriesPerSplit, schemaJson));
        }
        Collections.shuffle(splits);


        return new FixedSplitSource(splits);
    }

    private long getNumEntries(TopicName topicName, String zookeeperUri) throws Exception {

        ManagedLedgerFactory managedLedgerFactory = null;
        ManagedLedger managedLedger = null;
        ManagedCursor managedCursor = null;
        try {
            ClientConfiguration bkClientConfiguration = new ClientConfiguration().setZkServers(zookeeperUri);

            managedLedgerFactory = new ManagedLedgerFactoryImpl(bkClientConfiguration);

            ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1).setMetadataAckQuorumSize(1);


            managedLedger = managedLedgerFactory.open(topicName.getPersistenceNamingEncoding(), managedLedgerConfig);

            ManagedLedgerImpl managedLedgerImpl = (ManagedLedgerImpl) managedLedger;

            List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> managedListLedgers = managedLedgerImpl.getLedgersInfoAsList();
            log.info("managedListLedgers: %s", managedListLedgers);

            managedCursor = managedLedgerImpl.newNonDurableCursor(PositionImpl.earliest);
            long entries = managedCursor.getNumberOfEntriesInBacklog();

            log.info("num of entries: %s", entries);



            return entries;
        } finally {
            if (managedCursor != null) {
                try
                {
                    managedCursor.close();
                } catch (Exception e) {
                    log.error(e);
                }
            }

            if (managedLedger != null) {
                try {
                    managedLedger.close();
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
