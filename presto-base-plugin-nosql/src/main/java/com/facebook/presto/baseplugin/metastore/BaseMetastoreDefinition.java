package com.facebook.presto.baseplugin.metastore;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by amehta on 8/1/16.
 */
public interface BaseMetastoreDefinition {

    List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName);

    void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace);

    void dropView(ConnectorSession session, SchemaTableName viewName);

    Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix schemaTablePrefix);
}
