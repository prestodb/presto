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
package com.facebook.presto.iceberg.util;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Splitter;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Optional;

public class IcebergPrestoModelConverters
{
    private static final String NAMESPACE_SEPARATOR = ".";
    private static final Splitter NAMESPACE_SPLITTER = Splitter.on(NAMESPACE_SEPARATOR).omitEmptyStrings().trimResults();

    private IcebergPrestoModelConverters()
    {
    }

    public static String toPrestoSchemaName(Namespace icebergNamespace)
    {
        return icebergNamespace.toString();
    }

    public static Namespace toIcebergNamespace(Optional<String> prestoSchemaName)
    {
        if (prestoSchemaName.isPresent()) {
            return Namespace.of(NAMESPACE_SPLITTER.splitToList(prestoSchemaName.get()).toArray(new String[0]));
        }
        return Namespace.empty();
    }

    public static SchemaTableName toPrestoSchemaTableName(TableIdentifier icebergTableIdentifier)
    {
        return new SchemaTableName(icebergTableIdentifier.namespace().toString(), icebergTableIdentifier.name());
    }

    public static TableIdentifier toIcebergTableIdentifier(SchemaTableName prestoSchemaTableName)
    {
        return toIcebergTableIdentifier(toIcebergNamespace(Optional.ofNullable(prestoSchemaTableName.getSchemaName())), prestoSchemaTableName.getTableName());
    }

    public static TableIdentifier toIcebergTableIdentifier(Namespace icebergNamespace, String prestoTableName)
    {
        return TableIdentifier.of(icebergNamespace, prestoTableName);
    }
}
