package com.facebook.presto.operator.scalar;

import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import java.io.IOException;

@SuppressWarnings("UnusedDeclaration")
public final class JsonFunctions
{
    private JsonFunctions() {}

    @ScalarFunction("json_extract_scalar")
    public static Slice jsonExtractScalar(Slice json, Slice jsonPath)
    {
        try {
            return JsonExtract.extractScalar(json, jsonPath);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @ScalarFunction("json_extract")
    public static Slice jsonExtract(Slice json, Slice jsonPath)
    {
        try {
            return JsonExtract.extractJson(json, jsonPath);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
