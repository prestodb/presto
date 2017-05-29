/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.List;
import java.util.Map;

public final class BenchmarkUniqueNameUtils
{

    public static String generateBenchmarkUniqueName(String benchmarkName, Map<String, String> benchmarkVariables)
    {
        StringBuilder generatedName = new StringBuilder(benchmarkName);

        List<String> orderedVariableNames = ImmutableList.copyOf(Ordering.natural().sortedCopy(benchmarkVariables.keySet()));
        for (String variableName : orderedVariableNames) {
            generatedName.append('_');
            generatedName.append(variableName);
            generatedName.append('=');
            generatedName.append(benchmarkVariables.get(variableName));
        }

        // leaves in benchmark name only alphanumerics, underscores and dashes
        return generatedName.toString().replaceAll("[^A-Za-z0-9_=-]", "_");
    }

    private BenchmarkUniqueNameUtils()
    {
    }
}
