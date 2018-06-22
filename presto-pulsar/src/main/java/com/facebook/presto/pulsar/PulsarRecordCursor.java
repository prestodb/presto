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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.admin.shade.io.netty.buffer.Unpooled;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericDatumReader;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.shade.org.apache.avro.io.DatumReader;
import org.apache.pulsar.shade.org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;

public class PulsarRecordCursor implements RecordCursor {

    private final List<PulsarColumnHandle> columnHandles;
    private final PulsarSplit pulsarSplit;
    private final PulsarConnectorConfig pulsarConnectorConfig;
    private ManagedLedgerFactory managedLedgerFactory;
    private ManagedCursor managedCursor;
    private ManagedLedger managedLedger;
    private Queue<Entry> entryQueue = new LinkedList<>();
    private Entry currentEntry;
    private Schema schema;
    private GenericRecord currentRecord;
    private static final int NUM_ENTRY_READ_BATCH = 100;


    private static final Logger log = Logger.get(PulsarRecordCursor.class);

    public PulsarRecordCursor(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
            pulsarConnectorConfig) {
        this.columnHandles = columnHandles;
        this.pulsarSplit = pulsarSplit;
        this.pulsarConnectorConfig = pulsarConnectorConfig;

        this.schema = PulsarConnectorUtils.parseSchema(this.pulsarSplit.getSchema());

        int entryOffset = Math.toIntExact(pulsarSplit.getSplitSize() * pulsarSplit.getSplitId());

        try {
             this.managedCursor = getCursor(TopicName.get("persistent", NamespaceName.get(this.pulsarSplit.getSchemaName()), this.pulsarSplit.getTableName()), entryOffset, this.pulsarConnectorConfig.getZookeeperUri());
        } catch (Exception e) {
            log.error(e, "Failed to get cursor");
            throw new RuntimeException(e);
        }
    }

    private ManagedCursor getCursor(TopicName topicName, int entryOffset, String zookeeperUri) throws Exception {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration().setZkServers(zookeeperUri);

        managedLedgerFactory = new ManagedLedgerFactoryImpl(bkClientConfiguration);

        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1).setMetadataAckQuorumSize(1);

        managedLedger = managedLedgerFactory.open(topicName.getPersistenceNamingEncoding(), managedLedgerConfig);

        ManagedLedgerImpl managedLedgerImpl = (ManagedLedgerImpl) managedLedger;

        List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> managedListLedgers = managedLedgerImpl.getLedgersInfoAsList();
        log.info("managedListLedgers: {}", managedListLedgers);

        ManagedCursor cursor = managedLedgerImpl.newNonDurableCursor(PositionImpl.earliest);

        cursor.skipEntries(entryOffset, ManagedCursor.IndividualDeletedEntries.Include);

        return cursor;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    int counter = 0;
    int end = 10;
    @Override
    public boolean advanceNextPosition() {
        if (this.entryQueue.isEmpty()) {
            if (!this.managedCursor.hasMoreEntries()) {
                return false;
            }
            List<Entry> newEntries;
            try {
                newEntries = this.managedCursor.readEntries(NUM_ENTRY_READ_BATCH);
            } catch (InterruptedException | ManagedLedgerException e) {
                log.error(e, "Failed to read new entries from pulsar topic %s", TopicName.get("persistent", NamespaceName.get(this.pulsarSplit.getSchemaName()), this.pulsarSplit.getTableName()).toString());
                throw new RuntimeException(e);
            }

            this.entryQueue.addAll(newEntries);
        }

        this.currentEntry = this.entryQueue.poll();

        byte[] payload = Arrays.copyOfRange(this.currentEntry.getData(),44, this.currentEntry.getData().length);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(this.schema);
        try {
            currentRecord = datumReader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
            log.info("emp: %s", currentRecord);
        } catch (IOException e) {
            log.error(e);
        }

        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        log.info("getBoolean: %s", field);
        checkFieldType(field, boolean.class);

        return false;
    }

    @Override
    public long getLong(int field) {
        log.info("getLong: %s", field);
        checkFieldType(field, long.class);

        return (int) this.currentRecord.get(field);
    }

    @Override
    public double getDouble(int field) {
        log.info("getDouble: %s", field);
        checkFieldType(field, double.class);

        return counter;
    }

    @Override
    public Slice getSlice(int field) {
        log.info("getSlice: %s", field);
        checkFieldType(field, Slice.class);

        return Slices.utf8Slice(((org.apache.pulsar.shade.org.apache.avro.util.Utf8) this.currentRecord.get(field)).toString());
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        return false;
    }

    @Override
    public void close() {
        if (this.managedCursor != null) {
            try {
                this.managedCursor.close();
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

    private void checkFieldType(int field, Class<?> expected)
    {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }
}
