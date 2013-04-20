package com.facebook.presto.spi;

import java.util.List;
import java.util.Map;

public interface Plugin
{
    void setOptionalConfig(Map<String, String> optionalConfig);

    <T> List<T> getServices(Class<T> type);
}
