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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.plugin.jdbc.JdbcPlugin;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Sets;

import java.util.Set;

public class PostgreSqlPlugin
        extends JdbcPlugin
{
    public PostgreSqlPlugin()
    {
        super("postgresql", new PostgreSqlClientModule());
    }

    @Override
    public Iterable<Type> getTypes()
    {
        return Sets.newHashSet(PostgreSqlJsonType.POSTGRESQL_JSON, PostgreSqlJsonType.POSTGRESQL_JSONB);
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return Sets.newHashSet(PostgreSqlJsonFunctions.class);
    }
}
