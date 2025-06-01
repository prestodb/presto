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

package com.facebook.presto.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class PartitionFieldWrapper
{
    private final int sourceId;
    private final int fieldId;
    private final OptionalInt parameter;
    private final PartitionTransformType transform;
    private final String name;
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");

    @JsonCreator
    public PartitionFieldWrapper(
            @JsonProperty("sourceId") int sourceId,
            @JsonProperty("fieldId") int fieldId,
            @JsonProperty("parameter") OptionalInt parameter,
            @JsonProperty("transform") PartitionTransformType transform,
            @JsonProperty("name") String name)
    {
        this.sourceId = sourceId;
        this.fieldId = fieldId;
        this.parameter = parameter;
        this.transform = requireNonNull(transform, "transform is null");
        this.name = requireNonNull(name, "name is null");
    }

    @JsonProperty
    public int getSourceId()
    {
        return sourceId;
    }

    @JsonProperty
    public int getFieldId()
    {
        return fieldId;
    }

    @JsonProperty
    public OptionalInt getParameter()
    {
        return parameter;
    }

    @JsonProperty
    public PartitionTransformType getTransform()
    {
        return transform;
    }

    @JsonProperty
    public String getName()
    {
        return name;
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
        PartitionFieldWrapper that = (PartitionFieldWrapper) o;
        return transform == that.transform &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sourceId, fieldId, transform, name);
    }

    @Override
    public String toString()
    {
        return "PartitionField{" +
                "sourceId=" + sourceId +
                ", fieldId=" + fieldId +
                ", transform=" + transform +
                ", name='" + name + '\'' +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private int sourceId;
        private int fieldId;
        private OptionalInt parameter;
        private PartitionTransformType transform;
        private String name;

        public Builder setSourceId(int sourceId)
        {
            this.sourceId = sourceId;
            return this;
        }

        public Builder setFieldId(int fieldId)
        {
            this.fieldId = fieldId;
            return this;
        }

        public Builder setTransform(PartitionTransformType transform)
        {
            this.transform = transform;
            return this;
        }

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder setParameter(OptionalInt parameter)
        {
            this.parameter = parameter;
            return this;
        }

        public PartitionFieldWrapper build()
        {
            return new PartitionFieldWrapper(sourceId, fieldId, parameter, transform, name);
        }
    }
}
