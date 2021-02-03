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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.util.Failures;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFailureFunction
        extends AbstractTestFunctions
{
    private static final String FAILURE_INFO = JsonCodec.jsonCodec(FailureInfo.class).toJson(Failures.toFailure(new RuntimeException("fail me")).toFailureInfo());

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "fail me")
    public void testFailure()
    {
        assertFunction("fail(json_parse('" + FAILURE_INFO + "'))", UNKNOWN, null);
    }

    @Test
    public void testQuery()
    {
        // The other test does not exercise this function during execution (i.e. inside a page processor).
        // It only verifies constant folding works.
        try (LocalQueryRunner runner = new LocalQueryRunner(TEST_SESSION)) {
            try {
                runner.execute("select if(x, 78, 0/0) from (values rand() >= 0, rand() < 0) t(x)");
                throw new RuntimeException("Should throw PrestoException");
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), DIVISION_BY_ZERO.toErrorCode());
                assertTrue(e.getMessage().contains("/ by zero"));
            }
        }
    }
}
