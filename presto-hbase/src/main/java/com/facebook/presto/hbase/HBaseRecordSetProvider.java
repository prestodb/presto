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
package com.facebook.presto.hbase;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderRegistry;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.hbase.HBaseHandleResolver.convertColumnHandle;
import static com.facebook.presto.hbase.HBaseHandleResolver.convertSplit;
import static java.util.Objects.requireNonNull;

/**
 * Factory for HBase specific {@link RecordSet} instances.
 */
public class HBaseRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final HBaseSimpleConsumerManager consumerManager;
    private final DecoderRegistry registry;

    @Inject
    public HBaseRecordSetProvider(DecoderRegistry registry, HBaseSimpleConsumerManager consumerManager)
    {
        this.registry = requireNonNull(registry, "registry is null");
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        HBaseSplit hbaseSplit = convertSplit(split);

        ImmutableList.Builder<DecoderColumnHandle> handleBuilder = ImmutableList.builder();
        ImmutableMap.Builder<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoderBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoderBuilder = ImmutableMap.builder();

        RowDecoder keyDecoder = registry.getRowDecoder(hbaseSplit.getKeyDataFormat());
        RowDecoder messageDecoder = registry.getRowDecoder(hbaseSplit.getMessageDataFormat());

        for (ColumnHandle handle : columns) {
            HBaseColumnHandle columnHandle = convertColumnHandle(handle);
            handleBuilder.add(columnHandle);

            if (!columnHandle.isInternal()) {
                if (columnHandle.isKeyDecoder()) {
                    FieldDecoder<?> fieldDecoder = registry.getFieldDecoder(
                            hbaseSplit.getKeyDataFormat(),
                            columnHandle.getType().getJavaType(),
                            columnHandle.getDataFormat());

                    keyFieldDecoderBuilder.put(columnHandle, fieldDecoder);
                }
                else {
                    FieldDecoder<?> fieldDecoder = registry.getFieldDecoder(
                            hbaseSplit.getMessageDataFormat(),
                            columnHandle.getType().getJavaType(),
                            columnHandle.getDataFormat());

                    messageFieldDecoderBuilder.put(columnHandle, fieldDecoder);
                }
            }
        }

        ImmutableList<DecoderColumnHandle> handles = handleBuilder.build();
        ImmutableMap<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders = keyFieldDecoderBuilder.build();
        ImmutableMap<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders = messageFieldDecoderBuilder.build();

        return new HBaseRecordSet(hbaseSplit, consumerManager, handles, keyDecoder, messageDecoder, keyFieldDecoders, messageFieldDecoders);
    }
}
