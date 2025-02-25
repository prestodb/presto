
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

/*
import io.trino.spi.connector.CatalogHandle;
*/
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

// TODO: This class should be implemented via table function by connector.
// catalogHandle - came from catalogName at the split which doesn't seem to exist anymore.
// ConnectorTableFunctionHandle - Should have been added by table function implementation
// transaction handle - already exists. Though in different location to trino.
// It was a record but switched to public final class due to java 8 restrictions.
// Contains all metadata information for executing a table function.
public final class  TableFunctionHandle
{
        //CatalogHandle catalogHandle,
        ConnectorTableFunctionHandle functionHandle;
        ConnectorTransactionHandle transactionHandle;

    public TableFunctionHandle(ConnectorTableFunctionHandle functionHandle,
                               ConnectorTransactionHandle transactionHandle)
    {
        //requireNonNull(catalogHandle, "catalogHandle is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
    }

    /*
    public CatalogHandle getCatalogHandle() {
        return catalogHandle;
    }*/

    public ConnectorTableFunctionHandle getFunctionHandle() {
        return functionHandle;
    }

    public ConnectorTransactionHandle getTransactionHandle() {
        return transactionHandle;
    }

}
