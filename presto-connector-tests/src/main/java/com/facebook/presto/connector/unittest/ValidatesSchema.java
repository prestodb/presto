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
package com.facebook.presto.connector.unittest;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.collect.ImmutableList.toImmutableList;

public interface ValidatesSchema
        extends SPITest
{
    String onlySchemaName();

    @Override
    default SchemaTableName tableInDefaultSchema(String tableName)
    {
        return new SchemaTableName(onlySchemaName(), tableName);
    }

    @Override
    default void withSchemas(ConnectorSession session, List<String> schemaNames, Callable<Void> callable)
            throws Exception
    {
        String schemaName = Iterables.getOnlyElement(schemaNames.stream().distinct().collect(toImmutableList()));
        Preconditions.checkState(onlySchemaName().equals(schemaName), "Invalid schema name. Only " + onlySchemaName() + " is allowed");
        callable.call();
    }
}
