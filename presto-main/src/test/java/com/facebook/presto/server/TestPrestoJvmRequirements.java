package com.facebook.presto.server;

import org.testng.annotations.Test;

import static com.facebook.presto.server.PrestoJvmRequirements.verifyJvmRequirements;

public class TestPrestoJvmRequirements
{
    @Test
    public void testVerifyJvmRequirements()
            throws Exception
    {
        verifyJvmRequirements();
    }
}
