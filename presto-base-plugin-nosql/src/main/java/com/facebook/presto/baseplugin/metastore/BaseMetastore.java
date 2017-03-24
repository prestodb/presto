package com.facebook.presto.baseplugin.metastore;

import com.facebook.presto.baseplugin.BaseConfig;
import com.facebook.presto.baseplugin.BaseConnectorInfo;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.h2.jdbcx.JdbcConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 8/1/16.
 */
public class BaseMetastore implements BaseMetastoreDefinition {
    private final BaseConnectorInfo baseConnectorInfo;
    private final BaseConfig baseConfig;

    private JdbcConnectionPool metastoreConnection;

    public BaseMetastore(BaseConnectorInfo baseConnectorInfo, BaseConfig baseConfig){
        this.baseConnectorInfo = requireNonNull(baseConnectorInfo, "baseConnectorInfo is null");
        this.baseConfig = requireNonNull(baseConfig, "baseConfig is null");

        this.metastoreConnection = JdbcConnectionPool.create(baseConfig.getMetastoreJdbcUrl(Optional.empty())+"/"+baseConnectorInfo.getConnectorId(), baseConfig.getMetastoreUsername(Optional.empty()), baseConfig.getMetastorePassword(Optional.empty()));
        createViewsTable();
    }

    private void createViewsTable(){
        try {
            Connection connection = metastoreConnection.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("create table if not exists views (viewName varchar(255) primary key, viewData varchar(400000))");

            preparedStatement.execute();
            preparedStatement.close();
            connection.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    private String getViewName(SchemaTableName schemaTableName)
    {
        return baseConnectorInfo.getConnectorId()+"."+schemaTableName.toString();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName) {
        ImmutableList.Builder<SchemaTableName> viewNames = ImmutableList.builder();
        try{
            Splitter splitter = Splitter.on('.').trimResults().limit(3);
            Connection connection = metastoreConnection.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("select viewName from views where viewName like ?");
            preparedStatement.setString(1, baseConnectorInfo.getConnectorId()+"%");

            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()){
                List<String> splits = splitter.splitToList(rs.getString("viewName"));
                viewNames.add(new SchemaTableName(splits.get(1), splits.get(2)));
            }

            preparedStatement.close();
            connection.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
        return viewNames.build();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace) {
        try {
            Connection connection = metastoreConnection.getConnection();
            PreparedStatement preparedStatement = replace ? connection.prepareStatement("update views set viewData = ? where viewName = ?") : connection.prepareStatement("insert into views (viewData,viewName) values(?,?)");

            preparedStatement.setString(1, viewData);
            preparedStatement.setString(2, getViewName(viewName));

            preparedStatement.execute();
            preparedStatement.close();
            connection.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName) {
        try{
            Connection connection = metastoreConnection.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("delete from views where viewName = ?");
            preparedStatement.setString(1, getViewName(viewName));

            preparedStatement.execute();
            preparedStatement.close();
            connection.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix schemaTablePrefix) {
        SchemaTableName schemaTableName = new SchemaTableName(schemaTablePrefix.getSchemaName(), schemaTablePrefix.getTableName());

        Map<SchemaTableName, ConnectorViewDefinition> viewMap = new HashMap<>();
        try {
            Connection connection = metastoreConnection.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("select viewData from views where viewName = ?");
            preparedStatement.setString(1, getViewName(schemaTableName));

            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()){
                viewMap.put(schemaTableName, new ConnectorViewDefinition(schemaTableName, Optional.of(session.getIdentity().getUser()), rs.getString("viewData")));
            }

            preparedStatement.close();
            connection.close();
        }catch (SQLException e){
            e.printStackTrace();
        }

        return viewMap;
    }
}
