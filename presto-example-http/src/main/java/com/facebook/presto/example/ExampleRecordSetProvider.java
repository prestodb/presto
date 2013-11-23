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
package com.facebook.presto.example;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExampleRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;

    @Inject
    public ExampleRecordSetProvider(ExampleConnectorId connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof ExampleSplit && ((ExampleSplit) split).getConnectorId().equals(connectorId);
    }

    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        checkNotNull(split, "partitionChunk is null");
        checkArgument(split instanceof ExampleSplit);

        ExampleSplit exampleSplit = (ExampleSplit) split;
        checkArgument(exampleSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<ExampleColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            checkArgument(handle instanceof ExampleColumnHandle);
            handles.add((ExampleColumnHandle) handle);
        }

        return new ExampleRecordSet(exampleSplit, handles.build());
    }
}
