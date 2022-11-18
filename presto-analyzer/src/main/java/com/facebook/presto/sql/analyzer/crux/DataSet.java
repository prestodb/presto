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

import static java.util.Objects.requireNonNull;

public class DataSet
        extends Query
{
    private final DataSetKind dataSetKind;

    protected DataSet(DataSetKind dataSetKind, CodeLocation location, QuerySchema schema)
    {
        super(QueryKind.DATA_SET, location, schema);
        this.dataSetKind = requireNonNull(dataSetKind, "dataSetKind is null");
    }

    public DataSetKind getDataSetKind()
    {
        return dataSetKind;
    }

    public boolean isHive()
    {
        return this instanceof HiveDataSet;
    }

    public HiveDataSet asHive()
    {
        return (HiveDataSet) this;
    }
}
