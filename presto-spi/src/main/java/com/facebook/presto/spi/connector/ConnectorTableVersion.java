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
package com.facebook.presto.spi.connector;

import com.facebook.presto.common.Utils;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Primitives;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConnectorTableVersion
{
    public enum VersionType
    {
        TIMESTAMP,
        VERSION
    }
    public enum VersionOperator
    {
        EQUAL,
        LESS_THAN
    }
    private final VersionType versionType;
    private final VersionOperator versionOperator;
    private final Type versionExpressionType;
    private final Object tableVersion;

    public ConnectorTableVersion(VersionType versionType, VersionOperator versionOperator, Type versionExpressionType, Object tableVersion)
    {
        requireNonNull(versionType, "versionType is null");
        requireNonNull(versionOperator, "versionOperator is null");
        requireNonNull(versionExpressionType, "versionExpressionType is null");
        requireNonNull(tableVersion, "tableVersion is null");
        // Consumers unbox this to the type's native Java representation, so a mismatch has to fail
        // here rather than as a ClassCastException deep inside a connector.
        if (!Primitives.wrap(versionExpressionType.getJavaType()).isInstance(tableVersion)) {
            throw new IllegalArgumentException(format("Object '%s' does not match type %s", tableVersion, versionExpressionType.getJavaType()));
        }
        this.versionType = versionType;
        this.versionOperator = versionOperator;
        this.versionExpressionType = versionExpressionType;
        this.tableVersion = tableVersion;
    }

    /**
     * The version travels inside a table function handle, which is serialized into the plan
     * fragment sent to each task. The version value is a type-native Java object, so it round-trips
     * as a single-position block the way {@code ConstantExpression} does; serializing it as a bare
     * Object would let a small BIGINT come back as an Integer and break the unboxing casts.
     */
    @JsonCreator
    public static ConnectorTableVersion createConnectorTableVersion(
            @JsonProperty("versionType") VersionType versionType,
            @JsonProperty("versionOperator") VersionOperator versionOperator,
            @JsonProperty("versionExpressionType") Type versionExpressionType,
            @JsonProperty("tableVersionBlock") Block tableVersionBlock)
    {
        return new ConnectorTableVersion(
                versionType,
                versionOperator,
                versionExpressionType,
                Utils.blockToNativeValue(versionExpressionType, tableVersionBlock));
    }

    @JsonProperty
    public VersionType getVersionType()
    {
        return versionType;
    }

    @JsonProperty
    public VersionOperator getVersionOperator()
    {
        return versionOperator;
    }

    @JsonProperty
    public Type getVersionExpressionType()
    {
        return versionExpressionType;
    }

    @JsonProperty
    public Block getTableVersionBlock()
    {
        return Utils.nativeValueToBlock(versionExpressionType, tableVersion);
    }

    public Object getTableVersion()
    {
        return tableVersion;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ConnectorTableVersion that = (ConnectorTableVersion) other;
        return versionType == that.versionType
                && versionOperator == that.versionOperator
                && Objects.equals(versionExpressionType, that.versionExpressionType)
                && Objects.equals(tableVersion, that.tableVersion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(versionType, versionOperator, versionExpressionType, tableVersion);
    }

    @Override
    public String toString()
    {
        return new StringBuilder("ConnectorTableVersion{")
                .append("tableVersionType=").append(versionType)
                .append(", versionExpressionType=").append(versionExpressionType)
                .append(", tableVersion=").append(tableVersion)
                .append('}')
                .toString();
    }
}
