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
package com.facebook.presto.metadata;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;

@Immutable
public class QualifiedSchemaName
{
    private final String catalogName;
    private final String schemaName;

    public QualifiedSchemaName(String catalogName, String schemaName)
    {
        this.catalogName = checkCatalogName(catalogName);
        this.schemaName = checkSchemaName(schemaName);
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
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
        QualifiedSchemaName o = (QualifiedSchemaName) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName);
    }

    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName;
    }
}
