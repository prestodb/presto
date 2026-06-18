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

import org.jdbi.v3.core.config.JdbiConfig;
import org.jdbi.v3.sqlobject.customizer.SqlStatementCustomizer;
import org.jdbi.v3.sqlobject.customizer.SqlStatementCustomizerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static java.util.Objects.requireNonNull;

public class SqlFunctionsTableCustomizerFactory
        implements SqlStatementCustomizerFactory
{
    private static final String SQL_FUNCTIONS_TABLE_KEY = "sql_functions_table";

    @Override
    public SqlStatementCustomizer createForType(Annotation annotation, Class<?> sqlObjectType)
    {
        return statement -> statement.define(SQL_FUNCTIONS_TABLE_KEY, statement.getConfig(Config.class).getTableName());
    }

    @Override
    public SqlStatementCustomizer createForMethod(Annotation annotation, Class<?> sqlObjectType, Method method)
    {
        return createForType(annotation, sqlObjectType);
    }

    public static class Config
            implements JdbiConfig<Config>
    {
        private String tableName;

        public Config()
        {
        }

        private Config(Config other)
        {
            this.tableName = other.tableName;
        }

        public String getTableName()
        {
            return tableName;
        }

        public void setTableName(String tableName)
        {
            this.tableName = requireNonNull(tableName, "tableName is null");
        }

        @Override
        public Config createCopy()
        {
            return new Config(this);
        }
    }
}
