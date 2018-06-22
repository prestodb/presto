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
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PulsarRecordSet implements RecordSet {

    private final List<PulsarColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final PulsarSplit pulsarSplit;
    private final PulsarConnectorConfig pulsarConnectorConfig;
    
    public PulsarRecordSet(PulsarSplit split, List<PulsarColumnHandle> columnHandles, PulsarConnectorConfig
            pulsarConnectorConfig) {
        requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (PulsarColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();

        this.pulsarSplit = split;

        this.pulsarConnectorConfig = pulsarConnectorConfig;
    }


    @Override
    public List<Type> getColumnTypes() {
        return this.columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new PulsarRecordCursor(this.columnHandles, this.pulsarSplit, this.pulsarConnectorConfig);
    }
}
