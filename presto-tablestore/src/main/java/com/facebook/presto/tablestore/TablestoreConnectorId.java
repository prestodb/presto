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
package com.facebook.presto.tablestore;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TablestoreConnectorId
{
    public static final String TABLESTORE_CONNECTOR = "tablestore";

    private final String catalogId;

    public TablestoreConnectorId(String id)
    {
        this.catalogId = requireNonNull(id, "catalogId is null");
    }

    public String getCatalogId()
    {
        return catalogId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TablestoreConnectorId that = (TablestoreConnectorId) o;
        return Objects.equals(catalogId, that.catalogId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(catalogId);
    }

    @Override
    public String toString()
    {
        return "TablestoreConnectorId{" +
                "catalogId='" + catalogId + '\'' +
                '}';
    }
}
