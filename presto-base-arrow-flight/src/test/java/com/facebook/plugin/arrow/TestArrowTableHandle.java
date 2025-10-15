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
package com.facebook.plugin.arrow;

import com.facebook.airlift.testing.EquivalenceTester;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.plugin.arrow.ArrowMetadataUtil.TABLE_CODEC;
import static com.facebook.plugin.arrow.ArrowMetadataUtil.assertJsonRoundTrip;

public class TestArrowTableHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        List<ArrowColumnHandle> columnHandles = Arrays.asList(new ArrowColumnHandle("column1", BigintType.BIGINT), new ArrowColumnHandle("column2", VarcharType.VARCHAR));
        assertJsonRoundTrip(TABLE_CODEC, new ArrowTableHandle("schema", "table", Optional.of(columnHandles)));
    }

    @Test
    public void testEquivalence()
    {
        List<ArrowColumnHandle> columnHandles = Arrays.asList(new ArrowColumnHandle("column1", BigintType.BIGINT), new ArrowColumnHandle("column2", VarcharType.VARCHAR));
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new ArrowTableHandle("tm_engine", "employees", Optional.of(columnHandles))).check();
    }
}
