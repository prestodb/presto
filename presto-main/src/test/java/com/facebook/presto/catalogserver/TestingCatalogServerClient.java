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

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.transaction.TransactionInfo;

class TestingCatalogServerClient
        implements CatalogServerClient
{
    @Override
    public boolean schemaExists(TransactionInfo transactionInfo, SessionRepresentation session, CatalogSchemaName schema)
    {
        return false;
    }

    @Override
    public boolean catalogExists(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName)
    {
        return true;
    }

    @Override
    public String listSchemaNames(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName)
    {
        return "[\"information_schema\",\"tiny\",\"sf1\",\"sf100\",\"sf300\",\"sf1000\",\"sf3000\",\"sf10000\",\"sf30000\",\"sf100000\"]";
    }

    @Override
    public String getTableHandle(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName table)
    {
        return "{\"connectorId\":\"$info_schema@system\",\"connectorHandle\":{\"@type\":\"$info_schema\",\"catalogName\":\"system\",\"schemaName\":\"information_schema\",\"tableName\":\"schemata\"},\"transaction\":{\"@type\":\"$info_schema\",\"transactionId\":\"ffe9ae3e-60de-4175-a0b5-d635767085fa\"}}";
    }

    @Override
    public String listTables(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        return "[\"tpch.sf1.nation\"]";
    }

    @Override
    public String listViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        return "[\"hive.tpch.eric\",\"hive.tpch.eric2\"]";
    }

    @Override
    public String getViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        return "{\"hive.tpch.eric\":{\"originalSql\":\"SELECT name\\nFROM\\n  tpch.sf1.nation\\n\",\"catalog\":\"hive\",\"schema\":\"tpch\",\"columns\":[],\"owner\":\"ericn576\",\"runAsInvoker\":false}}";
    }

    @Override
    public String getView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName)
    {
        return "{\"originalSql\":\"SELECT name\\nFROM\\n  tpch.sf1.nation\\n\",\"catalog\":\"hive\",\"schema\":\"tpch\",\"columns\":[],\"owner\":\"ericn576\",\"runAsInvoker\":false}";
    }

    @Override
    public String getMaterializedView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName)
    {
        return "{\"originalSql\":\"SELECT\\n  name\\n, nationkey\\nFROM\\n  test_customer_base\\n\",\"schema\":\"tpch\",\"table\":\"eric\",\"baseTables\":[{\"schema\":\"tpch\",\"table\":\"test_customer_base\"}],\"owner\":\"ericn576\",\"columnMapping\":[{\"viewColumn\":{\"tableName\":{\"schema\":\"tpch\",\"table\":\"eric\"},\"columnName\":\"name\",\"isDirectMapped\":true},\"baseTableColumns\":[{\"tableName\":{\"schema\":\"tpch\",\"table\":\"test_customer_base\"},\"columnName\":\"name\",\"isDirectMapped\":true}]}],\"baseTablesOnOuterJoinSide\":[],\"validRefreshColumns\":[\"nationkey\"]}";
    }

    @Override
    public String getReferencedMaterializedViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName tableName)
    {
        return "[\"hive.tpch.test_customer_base\"]";
    }
}
