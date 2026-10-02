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
package com.facebook.presto.spi.derivedcolumns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * This class stores the derived column information.
 */
public class DerivedColumnSpec
{
    private final DerivedColumnType derivedColumnType;
    private final String derivedColumnExpression;
    private final String derivedColumnName;
    private final int derivedColumnFieldId;
    private final String derivedColumnReturnType;

    /**
     * The field derivedColumnType, derivedColumnFieldId and derivedColumnReturnType are used for validation only,
     * these values establish if derived column information has gone stale (by an external update) and needs a refresh.
     *
     * @param derivedColumnType A derived column can either be a GENERATED ALWAYS and PERSISTENT or VIRTUAL or just PERSISTENT.
     * @param derivedColumnExpression A derived column expression, a generic SQL expression that presto recognizes.
     * @param derivedColumnName Name of the derived column
     * @param derivedColumnFieldId field ID is connector dependent sequence number for the column.
     * @param derivedColumnReturnType return type of this column, used for validation purpose.
     */
    @JsonCreator
    public DerivedColumnSpec(
            @JsonProperty("derivedColumnType") DerivedColumnType derivedColumnType,
            @JsonProperty("derivedColumnExpression") String derivedColumnExpression,
            @JsonProperty("derivedColumnName") String derivedColumnName,
            @JsonProperty("derivedColumnFieldId") Integer derivedColumnFieldId,
            @JsonProperty("derivedColumnReturnType") String derivedColumnReturnType)
    {
        this.derivedColumnType = requireNonNull(derivedColumnType, "derivedColumnType is null");
        this.derivedColumnExpression = requireNonNull(derivedColumnExpression, "derivedColumnExpression is null");
        this.derivedColumnName = requireNonNull(derivedColumnName, "derivedColumnName is null");
        this.derivedColumnFieldId = requireNonNull(derivedColumnFieldId, "derivedColumnFieldId is null");
        this.derivedColumnReturnType = requireNonNull(derivedColumnReturnType, "derivedColumnReturnType is null");
    }

    @JsonProperty
    public DerivedColumnType getDerivedColumnType()
    {
        return derivedColumnType;
    }

    @JsonProperty
    public String getDerivedColumnExpression()
    {
        return derivedColumnExpression;
    }

    @JsonProperty
    public String getDerivedColumnName()
    {
        return derivedColumnName;
    }

    @JsonProperty
    public int getDerivedColumnFieldId()
    {
        return derivedColumnFieldId;
    }

    @JsonProperty
    public String getDerivedColumnReturnType()
    {
        return derivedColumnReturnType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DerivedColumnSpec that = (DerivedColumnSpec) o;
        return derivedColumnType == that.derivedColumnType
                && Objects.equals(derivedColumnExpression, that.derivedColumnExpression)
                && Objects.equals(derivedColumnName, that.derivedColumnName)
                && derivedColumnFieldId == that.derivedColumnFieldId
                && Objects.equals(derivedColumnReturnType, that.derivedColumnReturnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(derivedColumnType, derivedColumnExpression, derivedColumnName, derivedColumnFieldId, derivedColumnReturnType);
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder(this.getClass().getSimpleName());
        stringBuilder.append(" {")
                .append("derivedColumnType=").append(derivedColumnType)
                .append(", derivedColumnExpression='").append(derivedColumnExpression).append('\'')
                .append(", derivedColumnName='").append(derivedColumnName).append('\'')
                .append(", derivedColumnFieldId=").append(derivedColumnFieldId)
                .append(", derivedColumnReturnType='").append(derivedColumnReturnType).append('\'')
                .append('}');
        return stringBuilder.toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(DerivedColumnSpec derivedColumnSpec)
    {
        Builder builder = new Builder();
        builder.setDerivedColumnType(derivedColumnSpec.derivedColumnType)
                .setDerivedColumnName(derivedColumnSpec.derivedColumnName)
                .setDerivedColumnExpression(derivedColumnSpec.derivedColumnExpression)
                .setDerivedColumnFieldId(derivedColumnSpec.derivedColumnFieldId)
                .setDerivedColumnReturnType(derivedColumnSpec.derivedColumnReturnType);
        return builder;
    }

    public static class Builder
    {
        private DerivedColumnType derivedColumnType;
        private String derivedColumnExpression;
        private String derivedColumnName;
        private Integer derivedColumnFieldId;
        private String derivedColumnReturnType;

        public Builder setDerivedColumnName(String derivedColumnName)
        {
            this.derivedColumnName = derivedColumnName;
            return this;
        }

        public Builder setDerivedColumnExpression(String derivedColumnExpression)
        {
            this.derivedColumnExpression = derivedColumnExpression;
            return this;
        }

        public Builder setDerivedColumnType(DerivedColumnType derivedColumnType)
        {
            this.derivedColumnType = derivedColumnType;
            return this;
        }

        public Builder setDerivedColumnFieldId(Integer derivedColumnFieldId)
        {
            this.derivedColumnFieldId = derivedColumnFieldId;
            return this;
        }

        public Builder setDerivedColumnReturnType(String derivedColumnReturnType)
        {
            this.derivedColumnReturnType = derivedColumnReturnType;
            return this;
        }

        public DerivedColumnSpec build()
        {
            requireNonNull(derivedColumnReturnType, "derivedColumnReturnType is null");
            requireNonNull(derivedColumnFieldId, "derivedColumnFieldId is null");
            requireNonNull(derivedColumnType, "derivedColumnType is null");
            requireNonNull(derivedColumnName, "derivedColumnName is null");
            requireNonNull(derivedColumnExpression, "derivedColumnExpression is null");
            return new DerivedColumnSpec(derivedColumnType, derivedColumnExpression, derivedColumnName, derivedColumnFieldId, derivedColumnReturnType);
        }
    }
}
