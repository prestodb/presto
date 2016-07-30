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
package com.facebook.presto.accumulo.model;

import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.accumulo.AccumuloErrorCode.VALIDATION;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AccumuloSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String rowId;
    private final String schema;
    private final String table;
    private final String serializerClassName;
    private final Optional<String> scanAuthorizations;
    private final Optional<String> hostPort;
    private final List<HostAddress> addresses;
    private final List<AccumuloColumnConstraint> constraints;

    @JsonSerialize(contentUsing = RangeSerializer.class)
    @JsonDeserialize(contentUsing = RangeDeserializer.class)
    private List<Range> ranges;

    @JsonCreator
    public AccumuloSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("rowId") String rowId,
            @JsonProperty("serializerClassName") String serializerClassName,
            @JsonProperty("ranges") List<Range> ranges,
            @JsonProperty("constraints") List<AccumuloColumnConstraint> constraints,
            @JsonProperty("scanAuthorizations") Optional<String> scanAuthorizations,
            @JsonProperty("hostPort") Optional<String> hostPort)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.serializerClassName = requireNonNull(serializerClassName, "serializerClassName is null");
        this.constraints = ImmutableList.copyOf(requireNonNull(constraints, "constraints is null"));
        this.scanAuthorizations = requireNonNull(scanAuthorizations, "scanAuthorizations is null");
        this.hostPort = requireNonNull(hostPort, "hostPort is null");
        this.ranges = ImmutableList.copyOf(requireNonNull(ranges, "ranges is null"));

        // Parse the host address into a list of addresses, this would be an Accumulo Tablet server or some localhost thing
        if (hostPort.isPresent()) {
            addresses = ImmutableList.of(HostAddress.fromString(hostPort.get()));
        }
        else {
            addresses = ImmutableList.of();
        }
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public Optional<String> getHostPort()
    {
        return hostPort;
    }

    @JsonProperty
    public String getRowId()
    {
        return rowId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonIgnore
    public String getFullTableName()
    {
        return (this.getSchema().equals("default") ? "" : this.getSchema() + ".") + this.getTable();
    }

    @JsonProperty
    public String getSerializerClassName()
    {
        return this.serializerClassName;
    }

    @JsonProperty
    public List<Range> getRanges()
    {
        return ranges;
    }

    @JsonProperty
    public List<AccumuloColumnConstraint> getConstraints()
    {
        return constraints;
    }

    @SuppressWarnings("unchecked")
    @JsonIgnore
    public Class<? extends AccumuloRowSerializer> getSerializerClass()
    {
        try {
            return (Class<? extends AccumuloRowSerializer>) Class.forName(serializerClassName);
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(VALIDATION, "Configured serializer class not found", e);
        }
    }

    @JsonProperty
    public Optional<String> getScanAuthorizations()
    {
        return scanAuthorizations;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schema", schema)
                .add("table", table)
                .add("rowId", rowId)
                .add("serializerClassName", serializerClassName)
                .add("addresses", addresses)
                .add("numRanges", ranges.size())
                .add("constraints", constraints)
                .add("scanAuthorizations", scanAuthorizations)
                .add("hostPort", hostPort)
                .toString();
    }

    public static final class RangeSerializer
            extends JsonSerializer<Range>
    {
        private static final ThreadLocal<DataOutputBuffer> TL_OUT = new ThreadLocal<DataOutputBuffer>()
        {
            @Override
            protected DataOutputBuffer initialValue()
            {
                return new DataOutputBuffer();
            }
        };

        @Override
        public void serialize(Range value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            DataOutputBuffer out = TL_OUT.get();
            out.reset();
            value.write(out);
            jgen.writeBinary(out.getData(), 0, out.getLength());
        }
    }

    public static final class RangeDeserializer
            extends JsonDeserializer<Range>
    {
        private static final ThreadLocal<DataOutputBuffer> TL_OUT = new ThreadLocal<DataOutputBuffer>()
        {
            @Override
            protected DataOutputBuffer initialValue()
            {
                return new DataOutputBuffer();
            }
        };

        private static final ThreadLocal<DataInputBuffer> TL_BUFFER = new ThreadLocal<DataInputBuffer>()
        {
            @Override
            protected DataInputBuffer initialValue()
            {
                return new DataInputBuffer();
            }
        };

        @Override
        public Range deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException
        {
            DataOutputBuffer out = TL_OUT.get();
            DataInputBuffer buffer = TL_BUFFER.get();

            out.reset();
            jp.readBinaryValue(out);

            buffer.reset(out.getData(), out.getLength());
            Range r = new Range();
            r.readFields(buffer);
            return r;
        }
    }
}
