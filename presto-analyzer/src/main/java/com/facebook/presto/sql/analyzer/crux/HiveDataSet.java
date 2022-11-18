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
package com.facebook.presto.sql.analyzer.crux;

/**
 * TODO: This is dirty and hacky. This should be designed properly. Keywords like namespace should not exist.
 */
public class HiveDataSet
        extends DataSet
{
    private final String tableName;
    private final String namespace;

    // TODO: partition specs, bucketing, signal table, format, catalog
    public HiveDataSet(CodeLocation location, QuerySchema schema, String tableName, String namespace)
    {
        super(DataSetKind.HIVE, location, schema);
        this.tableName = tableName;
        this.namespace = namespace;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getNamespace()
    {
        return namespace;
    }
}
