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
package com.facebook.presto.kudu.schema;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.apache.kudu.client.KuduClient;

import java.util.List;

import static com.facebook.presto.kudu.KuduClientSession.DEFAULT_SCHEMA;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class NoSchemaEmulation
        implements SchemaEmulation
{
    @Override
    public void createSchema(KuduClient client, String schemaName)
    {
        if (DEFAULT_SCHEMA.equals(schemaName)) {
            throw new SchemaAlreadyExistsException(schemaName);
        }
        else {
            throw new PrestoException(GENERIC_USER_ERROR, "Creating schema in Kudu connector not allowed if schema emulation is disabled.");
        }
    }

    @Override
    public void dropSchema(KuduClient client, String schemaName)
    {
        if (DEFAULT_SCHEMA.equals(schemaName)) {
            throw new PrestoException(GENERIC_USER_ERROR, "Deleting default schema not allowed.");
        }
        else {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    @Override
    public boolean existsSchema(KuduClient client, String schemaName)
    {
        return DEFAULT_SCHEMA.equals(schemaName);
    }

    @Override
    public List<String> listSchemaNames(KuduClient client)
    {
        return ImmutableList.of("default");
    }

    @Override
    public String toRawName(SchemaTableName schemaTableName)
    {
        if (DEFAULT_SCHEMA.equals(schemaTableName.getSchemaName())) {
            return schemaTableName.getTableName();
        }
        else {
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }
    }

    @Override
    public SchemaTableName fromRawName(String rawName)
    {
        return new SchemaTableName(DEFAULT_SCHEMA, rawName);
    }

    @Override
    public String getPrefixForTablesOfSchema(String schemaName)
    {
        return "";
    }
}
