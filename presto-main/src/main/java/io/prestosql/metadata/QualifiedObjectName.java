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
package io.prestosql.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.SchemaTableName;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.metadata.MetadataUtil.checkObjectName;
import static java.util.Objects.requireNonNull;

@Immutable
public class QualifiedObjectName
{
    @JsonCreator
    public static QualifiedObjectName valueOf(String name)
    {
        requireNonNull(name, "name is null");

        ImmutableList<String> ids = ImmutableList.copyOf(Splitter.on('.').split(name));
        checkArgument(ids.size() == 3, "Invalid name %s", name);

        return new QualifiedObjectName(ids.get(0), ids.get(1), ids.get(2));
    }

    private final String catalogName;
    private final String schemaName;
    private final String objectName;

    public QualifiedObjectName(String catalogName, String schemaName, String objectName)
    {
        checkObjectName(catalogName, schemaName, objectName);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.objectName = objectName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getObjectName()
    {
        return objectName;
    }

    public SchemaTableName asSchemaTableName()
    {
        return new SchemaTableName(schemaName, objectName);
    }

    public CatalogSchemaTableName asCatalogSchemaTableName()
    {
        return new CatalogSchemaTableName(catalogName, schemaName, objectName);
    }

    public QualifiedTablePrefix asQualifiedTablePrefix()
    {
        return new QualifiedTablePrefix(catalogName, schemaName, objectName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QualifiedObjectName o = (QualifiedObjectName) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(objectName, o.objectName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, objectName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName + '.' + objectName;
    }

    public static Function<SchemaTableName, QualifiedObjectName> convertFromSchemaTableName(String catalogName)
    {
        return input -> new QualifiedObjectName(catalogName, input.getSchemaName(), input.getTableName());
    }
}
