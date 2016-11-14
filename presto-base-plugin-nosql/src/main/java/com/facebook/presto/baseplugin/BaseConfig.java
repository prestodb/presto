package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.ConnectorSession;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;
import java.util.Properties;

/**
 * Created by amehta on 6/13/16.
 */
public abstract class BaseConfig {
    private final Properties properties;

    public BaseConfig(){
        this.properties = new Properties();

        setProperty("basePlugin.cacheEnabled", false);
        setProperty("basePlugin.metastoreJdbcUrl", "jdbc:h2:~/h2/presto");
        setProperty("basePlugin.metastoreUsername", "sa");
        setProperty("basePlugin.metastorePassword", "sa");
        setProperty("basePlugin.defaultSchemaName", "default_schema");
    }

    protected void setProperty(String propertyName, Object value){
        this.properties.setProperty(propertyName, value.toString());
    }

    protected <T> T getProperty(String propertyName, Class<T> clazz, Optional<ConnectorSession> session){
        //return session.isPresent() ? BaseUtils.getPropertyFromSessionConfig(propertyName, clazz, session.get(), this) : BaseUtils.getPropertyFromMap(propertyName, clazz, properties);
        return BaseUtils.getPropertyFromMap(propertyName, clazz, properties);
    }

    @Config("basePlugin.cacheEnabled")
    @ConfigDescription("Whether the cache is enabled for this plugin")
    public void setCacheEnabled(Boolean cacheEnabled){
        setProperty("basePlugin.cacheEnabled", cacheEnabled);
    }

    public Boolean getCacheEnabled(Optional<ConnectorSession> session){
        return getProperty("basePlugin.cacheEnabled", Boolean.class, session);
    }

    public Boolean getCacheEnabled(){
        return getCacheEnabled(Optional.empty());
    }

    @Config("basePlugin.metastoreJdbcUrl")
    @ConfigDescription("the location of the metadata store")
    public void setMetastoreJdbcUrl(String metastoreJdbcUrl){
        setProperty("basePlugin.metastoreJdbcUrl", metastoreJdbcUrl);
    }

    public String getMetastoreJdbcUrl(Optional<ConnectorSession> session){
        return getProperty("basePlugin.metastoreJdbcUrl", String.class, session);
    }

    public String getMetastoreJdbcUrl(){
        return getMetastoreJdbcUrl(Optional.empty());
    }

    @Config("basePlugin.metastoreUsername")
    @ConfigDescription("the username for connecting to the metadata store")
    public void setMetastoreUsername(String metastoreUsername){
        setProperty("basePlugin.metastoreUsername", metastoreUsername);
    }

    public String getMetastoreUsername(Optional<ConnectorSession> session){
        return getProperty("basePlugin.metastoreUsername", String.class, session);
    }

    public String getMetastoreUsername(){
        return getMetastoreUsername(Optional.empty());
    }

    @Config("basePlugin.metastorePassword")
    @ConfigDescription("the password for connecting to the metadata store")
    public void setMetastorePassword(String metastorePassword){
        setProperty("basePlugin.metastorePassword", metastorePassword);
    }

    public String getMetastorePassword(Optional<ConnectorSession> session){
        return getProperty("basePlugin.metastorePassword", String.class, session);
    }

    public String getMetastorePassword(){
        return getMetastorePassword(Optional.empty());
    }

    @Config("basePlugin.defaultSchemaName")
    @ConfigDescription("the name of the default schema for datasources that don't have schemas")
    public void setDefaultSchemaName(String defaultSchemaName){
        setProperty("basePlugin.defaultSchemaName", defaultSchemaName);
    }

    public String getDefaultSchemaName(Optional<ConnectorSession> session){
        return getProperty("basePlugin.defaultSchemaName", String.class, session);
    }

    public String getDefaultSchemaName(){
        return getDefaultSchemaName(Optional.empty());
    }
}
