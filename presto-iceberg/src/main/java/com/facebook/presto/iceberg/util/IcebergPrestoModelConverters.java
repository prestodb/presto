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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Splitter;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

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

    public static Namespace toIcebergNamespace(Optional<String> prestoSchemaName, boolean nestedNamespaceEnabled)
    {
        if (prestoSchemaName.isPresent()) {
            checkNestedNamespaceSupport(prestoSchemaName.get(), nestedNamespaceEnabled);
            return Namespace.of(NAMESPACE_SPLITTER.splitToList(prestoSchemaName.get()).toArray(new String[0]));
        }
        return Namespace.empty();
    }

    public static SchemaTableName toPrestoSchemaTableName(TableIdentifier icebergTableIdentifier, boolean nestedNamespaceEnabled)
    {
        String schemaName = icebergTableIdentifier.namespace().toString();
        checkNestedNamespaceSupport(schemaName, nestedNamespaceEnabled);
        return new SchemaTableName(schemaName, icebergTableIdentifier.name());
    }

    public static TableIdentifier toIcebergTableIdentifier(SchemaTableName prestoSchemaTableName, boolean nestedNamespaceEnabled)
    {
        return toIcebergTableIdentifier(toIcebergNamespace(Optional.ofNullable(prestoSchemaTableName.getSchemaName()), nestedNamespaceEnabled),
                prestoSchemaTableName.getTableName(), nestedNamespaceEnabled);
    }

    public static TableIdentifier toIcebergTableIdentifier(Namespace icebergNamespace, String prestoTableName, boolean nestedNamespaceEnabled)
    {
        checkNestedNamespaceSupport(icebergNamespace.toString(), nestedNamespaceEnabled);
        return TableIdentifier.of(icebergNamespace, prestoTableName);
    }

    private static void checkNestedNamespaceSupport(String schemaName, boolean nestedNamespaceEnabled)
    {
        if (!nestedNamespaceEnabled && schemaName.contains(NAMESPACE_SEPARATOR)) {
            throw new PrestoException(NOT_SUPPORTED, format("Nested namespaces are disabled. Schema %s is not valid", schemaName));
        }
    }
}
