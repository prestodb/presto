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
package com.facebook.presto.hive;

import org.testng.annotations.Test;

import java.util.OptionalLong;

import static com.facebook.presto.hive.HiveBasicStatistics.createFromPartitionParameters;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveBasicStatistics
{
    @Test
    public void testRoundTrip()
    {
        testRoundTrip(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()));
        testRoundTrip(new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.empty(), OptionalLong.of(2), OptionalLong.empty()));
        testRoundTrip(new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)));
    }

    private static void testRoundTrip(HiveBasicStatistics expected)
    {
        assertThat(createFromPartitionParameters(expected.toPartitionParameters()))
                .isEqualTo(expected);
    }
}
