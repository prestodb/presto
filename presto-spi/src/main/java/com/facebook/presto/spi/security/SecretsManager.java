package com.facebook.presto.spi.security;

import java.util.Map;

public interface SecretsManager
{
    Map<String, String> resolveSecrets(String catalogName, Map<String, String> properties);
}
