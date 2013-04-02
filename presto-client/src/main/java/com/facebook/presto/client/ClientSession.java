/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

import com.google.common.base.Objects;

import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClientSession
{
    private URI server;
    private String user;
    private String catalog;
    private String schema;
    private boolean debug;

    public ClientSession(URI server, String user, String catalog, String schema, boolean debug)
    {
        this.server = checkNotNull(server, "server is null");
        this.user = user;
        this.catalog = catalog;
        this.schema = schema;
        this.debug = debug;
    }

    public URI getServer()
    {
        return server;
    }

    public void setServer(URI server)
    {
        this.server = checkNotNull(server, "server is null");
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public boolean isDebug()
    {
        return debug;
    }

    public void setDebug(boolean debug)
    {
        this.debug = debug;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("server", server)
                .add("user", user)
                .add("catalog", catalog)
                .add("schema", schema)
                .add("debug", debug)
                .toString();
    }
}
