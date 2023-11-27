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
package com.facebook.presto.client;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestResultForPrepare
{
    private static final JsonCodec<ResultForPrepare> RESULT_FOR_PREPARE_CODEC = jsonCodec(ResultForPrepare.class);

    @Test
    public void testCompatibility()
    {
        ResultForPrepare resultForPrepare = new ResultForPrepare(true,
                ImmutableList.of(
                        IntegerType.INTEGER.getTypeSignature().toString(),
                        BigintType.BIGINT.getTypeSignature().toString(),
                        VarcharType.createVarcharType(1024).getTypeSignature().toString()));
        String jsonStrValue = RESULT_FOR_PREPARE_CODEC.toJson(resultForPrepare);
        ResultForPrepare results = RESULT_FOR_PREPARE_CODEC.fromJson(jsonStrValue);

        assertEquals(results.isInsertValuesWithParameter(), true);
        assertEquals(results.getTypesForValidate(), ImmutableList.of(
                IntegerType.INTEGER.getTypeSignature().toString(),
                BigintType.BIGINT.getTypeSignature().toString(),
                VarcharType.createVarcharType(1024).getTypeSignature().toString()));
    }
}
