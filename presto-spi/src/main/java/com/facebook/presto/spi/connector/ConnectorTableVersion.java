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

import com.facebook.presto.common.type.Type;

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
        this.versionType = versionType;
        this.versionOperator = versionOperator;
        this.versionExpressionType = versionExpressionType;
        this.tableVersion = tableVersion;
    }

    public VersionType getVersionType()
    {
        return versionType;
    }

    public VersionOperator getVersionOperator()
    {
        return versionOperator;
    }

    public Type getVersionExpressionType()
    {
        return versionExpressionType;
    }

    public Object getTableVersion()
    {
        return tableVersion;
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
