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
package com.facebook.presto.catalogserver;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.transaction.TransactionInfo;

@ThriftService("PrestoCatalogServer")
public interface CatalogServerClient
{
    @ThriftMethod
    boolean schemaExists(TransactionInfo transactionInfo, SessionRepresentation session, CatalogSchemaName schema);

    @ThriftMethod
    boolean catalogExists(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName);

    @ThriftMethod
    String listSchemaNames(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName);

    @ThriftMethod
    String getTableHandle(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName table);

    @ThriftMethod
    String listTables(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix);

    @ThriftMethod
    String listViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix);

    @ThriftMethod
    String getViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix);

    @ThriftMethod
    String getView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName);

    @ThriftMethod
    String getMaterializedView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName);

    @ThriftMethod
    String getReferencedMaterializedViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName tableName);
}
