package com.facebook.presto.plugin.mysql;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.JdbcPageSinkProvider;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcSessionPropertiesProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.relation.RowExpressionService;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class MySqlConnector
    extends JdbcConnector
{
    public MySqlConnector(LifeCycleManager lifeCycleManager, JdbcMetadataFactory jdbcMetadataFactory, JdbcSplitManager jdbcSplitManager, JdbcRecordSetProvider jdbcRecordSetProvider, JdbcPageSinkProvider jdbcPageSinkProvider, Optional<ConnectorAccessControl> accessControl, Set<Procedure> procedures, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution, RowExpressionService rowExpressionService, JdbcClient jdbcClient, Optional<JdbcSessionPropertiesProvider> sessionPropertiesProvider)
    {
        super(lifeCycleManager, jdbcMetadataFactory, jdbcSplitManager, jdbcRecordSetProvider, jdbcPageSinkProvider, accessControl, procedures, functionManager, functionResolution, rowExpressionService, jdbcClient, sessionPropertiesProvider);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        JdbcMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader());
    }
}
