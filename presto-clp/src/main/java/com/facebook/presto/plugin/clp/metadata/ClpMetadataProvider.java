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
package com.facebook.presto.plugin.clp.metadata;

import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;

/**
 * A provider for metadata that describes what tables exist in the CLP connector, and what columns
 * exist in each of those tables.
 */
public interface ClpMetadataProvider
{
    /**
     * Returns the list of schema names available in this connector.
     * <p>
     * The default implementation returns only the default schema. Implementations can override
     * this method to support multiple schemas by querying their metadata source (e.g., YAML file
     * or database) to discover available schemas.
     *
     * @return the list of schema names available in this connector
     */
    default List<String> listSchemaNames()
    {
        return ImmutableList.of(DEFAULT_SCHEMA_NAME);
    }

    /**
     * @param schemaTableName the name of the schema and the table
     * @return the list of column handles for the given table.
     */
    List<ClpColumnHandle> listColumnHandles(SchemaTableName schemaTableName);

    /**
     * @param schema the name of the schema
     * @return the list of table handles in the specified schema
     */
    List<ClpTableHandle> listTableHandles(String schema);
}
