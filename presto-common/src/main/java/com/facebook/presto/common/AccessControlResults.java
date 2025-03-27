package com.facebook.presto.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AccessControlResults
{
    Map<String, String> results = new HashMap<>();

    @JsonCreator
    public AccessControlResults() {

    }

    public void logToAccessControlResultsHash(String message, String value) {
        results.put(message, value);
    }

    @JsonValue
    public Map<String, String> getAccessControlResultsHash()
    {
        return Collections.unmodifiableMap(results);
    }
}
