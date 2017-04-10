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
package com.facebook.presto.localfile;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.localfile.MetadataUtil.COLUMN_CODEC;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestLocalFileColumnHandle
{
    private final List<LocalFileColumnHandle> columnHandle = ImmutableList.of(
            new LocalFileColumnHandle("columnName", createUnboundedVarcharType(), 0),
            new LocalFileColumnHandle("columnName", BIGINT, 0),
            new LocalFileColumnHandle("columnName", DOUBLE, 0),
            new LocalFileColumnHandle("columnName", DATE, 0),
            new LocalFileColumnHandle("columnName", TIMESTAMP, 0),
            new LocalFileColumnHandle("columnName", BOOLEAN, 0));

    @Test
    public void testJsonRoundTrip()
    {
        for (LocalFileColumnHandle handle : columnHandle) {
            String json = COLUMN_CODEC.toJson(handle);
            LocalFileColumnHandle copy = COLUMN_CODEC.fromJson(json);
            assertEquals(copy, handle);
        }
    }
}
