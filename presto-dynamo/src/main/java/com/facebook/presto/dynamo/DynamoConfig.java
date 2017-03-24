package com.facebook.presto.dynamo;

import com.facebook.presto.baseplugin.BaseConfig;
import com.facebook.presto.spi.ConnectorSession;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

/**
 * Created by amehta on 6/14/16.
 */
public class DynamoConfig extends BaseConfig {
    @Config("dynamo.awsKey")
    @ConfigDescription("access key for AWS")
    public void setAwsKey(String awsKey){
        setProperty("dynamo.awsKey", awsKey);
    }

    public String getAwsKey(Optional<ConnectorSession> session){
        return getProperty("dynamo.awsKey", String.class, session);
    }

    public String getAwsKey(){
        return getAwsKey(Optional.empty());
    }

    @Config("dynamo.secretKey")
    @ConfigDescription("secret key for AWS")
    public void setSecretKey(String secretKey){
        setProperty("dynamo.secretKey", secretKey);
    }

    public String getSecretKey(Optional<ConnectorSession> session){
        return getProperty("dynamo.secretKey", String.class, session);
    }

    public String getSecretKey(){
        return getSecretKey(Optional.empty());
    }

    @Config("dynamo.lookupSuffix")
    @ConfigDescription("suffix for lookup tables")
    public void setLookupSuffix(String lookupSuffix){
        setProperty("dynamo.lookupSuffix", lookupSuffix);
    }

    public String getLookupSuffix(Optional<ConnectorSession> session){
        return getProperty("dynamo.lookupSuffix", String.class, session);
    }

    public String getLookupSuffix(){
        return getLookupSuffix(Optional.empty());
    }

    @Config("dynamo.lookupColumnName")
    @ConfigDescription("name of attribute that represents column name in lookup table")
    public void setLookupColumnName(String lookupColumnName){
        setProperty("dynamo.lookupColumnName", lookupColumnName);
    }

    public String getLookupColumnName(Optional<ConnectorSession> session){
        return getProperty("dynamo.lookupColumnName", String.class, session);
    }

    public String getLookupColumnName(){
        return getLookupColumnName(Optional.empty());
    }

    @Config("dynamo.lookupColumnType")
    @ConfigDescription("name of attribute that represents column type in lookup table")
    public void setLookupColumnType(String lookupColumnType){
        setProperty("dynamo.lookupColumnType", lookupColumnType);
    }

    public String getLookupColumnType(Optional<ConnectorSession> session){
        return getProperty("dynamo.lookupColumnType", String.class, session);
    }

    public String getLookupColumnType(){
        return getLookupColumnType(Optional.empty());
    }

    @Config("dynamo.maxRetries")
    @ConfigDescription("max retries for exponential backoff")
    public void setMaxRetries(Integer maxRetries){
        setProperty("dynamo.maxRetries", maxRetries);
    }

    public Integer getMaxRetries(Optional<ConnectorSession> session){
        return getProperty("dynamo.maxRetries", Integer.class, session);
    }

    public Integer getMaxRetries(){
        return getMaxRetries(Optional.empty());
    }
}
