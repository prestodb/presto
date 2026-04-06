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
package com.facebook.presto.iceberg.partitioning;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.iceberg.PartitionTransformType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.PartitionField;

import java.util.OptionalInt;
import java.util.regex.Matcher;

import static com.facebook.presto.iceberg.PartitionFields.ICEBERG_BUCKET_PATTERN;
import static com.facebook.presto.iceberg.PartitionFields.ICEBERG_TRUNCATE_PATTERN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IcebergPartitionFieldHandle
{
    final int sourceColumnChannel;
    final PartitionTransformType transform;
    final Type type;
    final OptionalInt size;
    final String partitionFieldMessage;

    public IcebergPartitionFieldHandle(int sourceColumnChannel, PartitionField partitionField, Type type)
    {
        this(sourceColumnChannel, partitionField, type, OptionalInt.empty());
    }

    public IcebergPartitionFieldHandle(
            int sourceColumnChannel,
            PartitionField partitionField,
            Type type,
            OptionalInt size)
    {
        this(sourceColumnChannel, PartitionTransformType.fromStringOrFail(partitionField.transform().toString()), partitionField.toString(), type, size);
    }

    @JsonCreator
    public IcebergPartitionFieldHandle(
            @JsonProperty("sourceColumnChannel") int sourceColumnChannel,
            @JsonProperty("transform") PartitionTransformType transform,
            @JsonProperty("partitionFieldMessage") String partitionFieldMessage,
            @JsonProperty("type") Type type,
            @JsonProperty("size") OptionalInt size)
    {
        checkArgument(sourceColumnChannel >= 0, "sourceColumnChannel must be greater than or equal to zero");
        this.sourceColumnChannel = sourceColumnChannel;
        this.transform = requireNonNull(transform, "transform is null");
        this.partitionFieldMessage = requireNonNull(partitionFieldMessage, "partitionFieldMessage is null");
        this.type = requireNonNull(type, "type is null");
        this.size = requireNonNull(size, "size is null");
        checkArgument(size.isEmpty() || size.getAsInt() >= 0, "size must be greater than or equal to zero");
        checkArgument(size.isPresent() && isParameterizedTransform() || size.isEmpty() && !isParameterizedTransform(), "size is only valid for BUCKET and TRUNCATE transforms");
    }

    @JsonProperty
    public int getSourceColumnChannel()
    {
        return sourceColumnChannel;
    }

    @JsonProperty
    public PartitionTransformType getTransform()
    {
        return transform;
    }

    @JsonProperty
    public String getPartitionFieldMessage()
    {
        return partitionFieldMessage;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public OptionalInt getSize()
    {
        return size;
    }

    @JsonIgnore
    public boolean isBucketTransform()
    {
        return transform == PartitionTransformType.BUCKET;
    }

    @JsonIgnore
    public boolean isParameterizedTransform()
    {
        return transform == PartitionTransformType.BUCKET || transform == PartitionTransformType.TRUNCATE;
    }

    @JsonIgnore
    public String getTransformString()
    {
        switch (transform) {
            case IDENTITY :
            case YEAR:
            case MONTH:
            case DAY:
            case HOUR:
                return transform.getTransform();
            case BUCKET:
            case TRUNCATE:
                checkArgument(size.isPresent(), "size must be present for BUCKET and TRUNCATE transforms");
                return transform.getTransform() + "[" + size.getAsInt() + "]";
            default:
                throw new UnsupportedOperationException("Unsupported partition transform: " + transform);
        }
    }

    public static IcebergPartitionFieldHandle create(int sourceColumnChannel, PartitionField partitionField, String transform, Type type)
    {
        switch (transform) {
            case "identity":
            case "year":
            case "month":
            case "day":
            case "hour":
            case "void":
                return new IcebergPartitionFieldHandle(sourceColumnChannel, partitionField, type);
            default:
                Matcher matcher = ICEBERG_BUCKET_PATTERN.matcher(transform);
                if (matcher.matches()) {
                    return new IcebergPartitionFieldHandle(
                            sourceColumnChannel,
                            partitionField,
                            type,
                            OptionalInt.of(Integer.parseInt(matcher.group(1))));
                }

                matcher = ICEBERG_TRUNCATE_PATTERN.matcher(transform);
                if (matcher.matches()) {
                    return new IcebergPartitionFieldHandle(
                            sourceColumnChannel,
                            partitionField,
                            type,
                            OptionalInt.of(Integer.parseInt(matcher.group(1))));
                }
                throw new UnsupportedOperationException("Unsupported partition transform: " + transform);
        }
    }
}
