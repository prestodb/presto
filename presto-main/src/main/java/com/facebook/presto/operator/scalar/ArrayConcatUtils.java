/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.server.SliceSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.common.base.Throwables;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.type.ArrayType.toStackRepresentation;

public final class ArrayConcatUtils
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));
    private static final CollectionType COLLECTION_TYPE = OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, Object.class);

    private ArrayConcatUtils() {}

    public static Slice concat(Slice left, Slice right)
    {
        List<Object> leftArray = readArray(left);
        List<Object> rightArray = readArray(right);
        List<Object> result = new ArrayList<>(leftArray.size() + rightArray.size()); // allow nulls
        result.addAll(leftArray);
        result.addAll(rightArray);
        return toStackRepresentation(result);
    }

    private static Slice concatElement(Slice in, Object value, boolean append)
    {
        List<Object> array = readArray(in);
        List<Object> result = new ArrayList<>(array.size() + 1); // allow nulls
        if (append) {
            result.addAll(array);
            result.add(value);
        }
        else {
            result.add(value);
            result.addAll(array);
        }
        return toStackRepresentation(result);
    }

    public static Slice appendElement(Slice in, Object value)
    {
        return concatElement(in, value, true);
    }

    public static Slice appendElement(Slice in, long value)
    {
        return appendElement(in, Long.valueOf(value));
    }

    public static Slice appendElement(Slice in, boolean value)
    {
        return appendElement(in, Boolean.valueOf(value));
    }

    public static Slice appendElement(Slice in, double value)
    {
        return appendElement(in, Double.valueOf(value));
    }

    public static Slice appendElement(Slice in, Slice value)
    {
        return concatElement(in, value, true);
    }

    public static Slice prependElement(Slice value, Slice in)
    {
        return concatElement(in, value, false);
    }

    public static Slice prependElement(Object value, Slice in)
    {
        return concatElement(in, value, false);
    }

    public static Slice prependElement(long value, Slice in)
    {
        return prependElement(Long.valueOf(value), in);
    }

    public static Slice prependElement(boolean value, Slice in)
    {
        return prependElement(Boolean.valueOf(value), in);
    }

    public static Slice prependElement(double value, Slice in)
    {
        return prependElement(Double.valueOf(value), in);
    }

    private static List<Object> readArray(Slice json)
    {
        try {
            return OBJECT_MAPPER.readValue(json.getInput(), COLLECTION_TYPE);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
