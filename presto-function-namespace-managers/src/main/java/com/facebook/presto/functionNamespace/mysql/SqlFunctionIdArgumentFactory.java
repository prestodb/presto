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
package com.facebook.presto.functionNamespace.mysql;

import com.facebook.presto.spi.function.SqlFunctionId;
import org.jdbi.v3.core.argument.AbstractArgumentFactory;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.argument.ObjectArgument;
import org.jdbi.v3.core.config.ConfigRegistry;

import static java.sql.Types.VARCHAR;

public class SqlFunctionIdArgumentFactory
        extends AbstractArgumentFactory<SqlFunctionId>
{
    public SqlFunctionIdArgumentFactory()
    {
        super(VARCHAR);
    }

    @Override
    public Argument build(SqlFunctionId value, ConfigRegistry config)
    {
        return new ObjectArgument(value.getId(), VARCHAR);
    }
}
