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
package com.facebook.presto.verifier.resolver;

import com.facebook.presto.verifier.framework.PrestoQueryException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERATED_BYTECODE_TOO_LARGE;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_CHECKSUM;
import static com.facebook.presto.verifier.prestoaction.QueryActionStats.EMPTY_STATS;
import static org.testng.Assert.assertEquals;

public class TestVerifierLimitationFailureResolver
        extends AbstractTestPrestoQueryFailureResolver
{
    public TestVerifierLimitationFailureResolver()
    {
        super(new VerifierLimitationFailureResolver());
    }

    @Test
    public void testResolveCompilerError()
    {
        assertEquals(
                getFailureResolver().resolveQueryFailure(
                        CONTROL_QUERY_STATS,
                        new PrestoQueryException(
                                new RuntimeException(),
                                false,
                                CONTROL_CHECKSUM,
                                Optional.of(COMPILER_ERROR),
                                EMPTY_STATS),
                        Optional.empty()),
                Optional.of("Checksum query too large"));
    }

    @Test
    public void testResolveGeneratedBytecodeTooLarge()
    {
        assertEquals(
                getFailureResolver().resolveQueryFailure(
                        CONTROL_QUERY_STATS,
                        new PrestoQueryException(
                                new RuntimeException(),
                                false,
                                CONTROL_CHECKSUM,
                                Optional.of(GENERATED_BYTECODE_TOO_LARGE),
                                EMPTY_STATS),
                        Optional.empty()),
                Optional.of("Checksum query too large"));
    }
}
