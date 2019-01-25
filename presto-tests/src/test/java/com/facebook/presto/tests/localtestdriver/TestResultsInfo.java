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
package com.facebook.presto.tests.localtestdriver;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.testng.Assert.assertEquals;

public class TestResultsInfo
{
    private static final JsonCodec<ResultInfo> RESULT_INFO_JSON_CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(ResultInfo.class);

    @Test
    public void testResultsInfo()
    {
        ResultInfo expected = new ResultInfo(Optional.of("updateType"), Optional.of(5L), ImmutableList.of(4L, 5L, 6L), 19L);
        byte[] resultInfoBytes = RESULT_INFO_JSON_CODEC.toJsonBytes(expected);
        ResultInfo actual = RESULT_INFO_JSON_CODEC.fromJson(resultInfoBytes);
        assertEquals(actual, expected);
    }
}
