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
package com.facebook.presto.sql.analyzer.utils;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_NOT_SPECIFIED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MetadataUtils
{
    private MetadataUtils()
    {}

    public static QualifiedObjectName createQualifiedObjectName(Optional<String> sessionCatalogName, Optional<String> sessionSchemaName, Node node, QualifiedName name,
                                                                BiFunction<String, String, String> normalizer)
    {
        requireNonNull(sessionCatalogName, "sessionCatalogName is null");
        requireNonNull(sessionSchemaName, "sessionSchemaName is null");
        requireNonNull(name, "name is null");
        if (name.getParts().size() > 3) {
            throw new PrestoException(SYNTAX_ERROR, format("Too many dots in table name: %s", name));
        }

        List<Identifier> parts = Lists.reverse(name.getOriginalParts());
        String objectName = parts.get(0).getValue();
        String schemaName = (parts.size() > 1) ? parts.get(1).getValue() : sessionSchemaName.orElseThrow(() ->
                new SemanticException(SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set"));
        String catalogName = (parts.size() > 2) ? parts.get(2).getValue() : sessionCatalogName.orElseThrow(() ->
                new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set"));

        catalogName = catalogName.toLowerCase(ENGLISH);
        schemaName = normalizer.apply(catalogName, schemaName);
        objectName = normalizer.apply(catalogName, objectName);
        return new QualifiedObjectName(catalogName, schemaName, objectName);
    }
}
