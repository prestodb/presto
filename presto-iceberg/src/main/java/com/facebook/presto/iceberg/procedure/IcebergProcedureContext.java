package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.spi.connector.ConnectorProcedureContext;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import static java.util.Objects.requireNonNull;

/**
 * Simple context wrapper for Iceberg distributed procedures.
 *
 * Carries Iceberg Table + Transaction through the procedure lifecycle:
 * begin → worker → finish
 */
public class IcebergProcedureContext
        implements ConnectorProcedureContext
{
    private final Table table;
    private final Transaction transaction;

    public IcebergProcedureContext(Table table, Transaction transaction)
    {
        this.table = requireNonNull(table, "table is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
    }

    public Table getTable()
    {
        return table;
    }

    public Transaction getTransaction()
    {
        return transaction;
    }
}
