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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class StreamDescriptorFactory
{
    private static final int ROOT_ID = 0;

    private StreamDescriptorFactory()
    {
    }

    public static StreamDescriptor createStreamDescriptor(List<OrcType> types, OrcDataSource dataSource)
    {
        ImmutableMap.Builder<Integer, StreamProperty> propertiesBuilder = ImmutableMap.builderWithExpectedSize(types.size());
        addOrcType("", "", ROOT_ID, types, propertiesBuilder);
        AllStreams allStreams = new AllStreams(dataSource, propertiesBuilder.build());
        return new StreamDescriptor(ROOT_ID, DEFAULT_SEQUENCE_ID, allStreams);
    }

    private static void addOrcType(String parentStreamName, String fieldName, int typeId, List<OrcType> types, ImmutableMap.Builder<Integer, StreamProperty> propertiesBuilder)
    {
        OrcType type = types.get(typeId);

        if (!fieldName.isEmpty()) {
            parentStreamName += "." + fieldName;
        }

        ImmutableList.Builder<Integer> nestedStreamIdsBuilder = ImmutableList.builderWithExpectedSize(type.getFieldCount());
        if (type.getOrcTypeKind() == STRUCT) {
            for (int i = 0; i < type.getFieldCount(); i++) {
                nestedStreamIdsBuilder.add(type.getFieldTypeIndex(i));
                addOrcType(parentStreamName, type.getFieldName(i), type.getFieldTypeIndex(i), types, propertiesBuilder);
            }
        }
        else if (type.getOrcTypeKind() == OrcType.OrcTypeKind.LIST) {
            nestedStreamIdsBuilder.add(type.getFieldTypeIndex(0));
            addOrcType(parentStreamName, "item", type.getFieldTypeIndex(0), types, propertiesBuilder);
        }
        else if (type.getOrcTypeKind() == OrcType.OrcTypeKind.MAP) {
            nestedStreamIdsBuilder.add(type.getFieldTypeIndex(0));
            nestedStreamIdsBuilder.add(type.getFieldTypeIndex(1));
            addOrcType(parentStreamName, "key", type.getFieldTypeIndex(0), types, propertiesBuilder);
            addOrcType(parentStreamName, "value", type.getFieldTypeIndex(1), types, propertiesBuilder);
        }
        StreamProperty streamProperty = new StreamProperty(parentStreamName, type, fieldName, nestedStreamIdsBuilder.build());
        propertiesBuilder.put(typeId, streamProperty);
    }

    public static class StreamProperty
    {
        private final String streamName;
        private final OrcType orcType;
        private final String fieldName;
        private final List<Integer> nestedStreamIds;

        public StreamProperty(String streamName, OrcType orcType, String fieldName, List<Integer> nestedStreamIds)
        {
            this.streamName = requireNonNull(streamName, "streamName is null");
            this.orcType = requireNonNull(orcType, "orcType is null");
            this.fieldName = requireNonNull(fieldName, "fieldName is null");
            this.nestedStreamIds = ImmutableList.copyOf(requireNonNull(nestedStreamIds, "nestedStreamIds is null"));
        }

        public String getStreamName()
        {
            return streamName;
        }

        public OrcType getOrcType()
        {
            return orcType;
        }

        public String getFieldName()
        {
            return fieldName;
        }

        public List<Integer> getNestedStreamIds()
        {
            return nestedStreamIds;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StreamProperty that = (StreamProperty) o;
            return Objects.equals(streamName, that.streamName)
                    && Objects.equals(orcType, that.orcType)
                    && Objects.equals(fieldName, that.fieldName)
                    && Objects.equals(nestedStreamIds, that.nestedStreamIds);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(streamName, orcType, fieldName, nestedStreamIds);
        }
    }

    public static class AllStreams
    {
        private final OrcDataSource orcDataSource;
        private final Map<Integer, StreamProperty> streamIdToProperties;

        public AllStreams(OrcDataSource orcDataSource, Map<Integer, StreamProperty> streamIdToProperties)
        {
            this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
            this.streamIdToProperties = ImmutableMap.copyOf(requireNonNull(streamIdToProperties, "streamProperties is null"));
        }

        public OrcDataSource getOrcDataSource()
        {
            return orcDataSource;
        }

        public StreamProperty getStreamProperty(int streamId)
        {
            StreamProperty streamProperty = streamIdToProperties.get(streamId);
            checkState(streamProperty != null, "StreamId %s is missing", streamId);
            return streamProperty;
        }
    }
}
