/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.spi;

import java.util.Map;

public interface ImportClientFactoryFactory
{
    String getConfigName();

    ImportClientFactory createImportClientFactory(Map<String, String> requiredConfig, Map<String, String> optionalConfig);
}
