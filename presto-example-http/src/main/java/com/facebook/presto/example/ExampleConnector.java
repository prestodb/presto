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

import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ReadOnlySafeConnector;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExampleConnector
        extends ReadOnlySafeConnector<ExampleTableHandle, ExampleColumnHandle, ExampleSplit, ConnectorIndexHandle, ExamplePartition>
{
    private final ExampleMetadata metadata;
    private final ExampleSplitManager splitManager;
    private final ExampleRecordSetProvider recordSetProvider;

    @Inject
    public ExampleConnector(
            ExampleMetadata metadata,
            ExampleSplitManager splitManager,
            ExampleRecordSetProvider recordSetProvider)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.recordSetProvider = checkNotNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ExampleMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public ExampleSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ExampleRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }
}
