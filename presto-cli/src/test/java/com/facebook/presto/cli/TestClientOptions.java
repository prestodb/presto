package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestClientOptions
{
    @Test
    public void testDefault()
    {
        ClientSession session = new ClientOptions().toClientSession();
        assertEquals(session.getServer().toString(), "http://localhost:8080");
    }

    @Test
    public void testServerHostOnly()
    {
        ClientOptions options = new ClientOptions();
        options.server = "localhost";
        ClientSession session = options.toClientSession();
        assertEquals(session.getServer().toString(), "http://localhost:80");
    }

    @Test
    public void testServerHostPort()
    {
        ClientOptions options = new ClientOptions();
        options.server = "localhost:8888";
        ClientSession session = options.toClientSession();
        assertEquals(session.getServer().toString(), "http://localhost:8888");
    }

    @Test
    public void testServerHttpUri()
    {
        ClientOptions options = new ClientOptions();
        options.server = "http://localhost/foo";
        ClientSession session = options.toClientSession();
        assertEquals(session.getServer().toString(), "http://localhost/foo");
    }

    @Test
    public void testServerHttpsUri()
    {
        ClientOptions options = new ClientOptions();
        options.server = "https://localhost/foo";
        ClientSession session = options.toClientSession();
        assertEquals(session.getServer().toString(), "https://localhost/foo");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidServer()
    {
        ClientOptions options = new ClientOptions();
        options.server = "x:y";
        options.toClientSession();
    }
}
