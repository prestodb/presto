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
package com.facebook.presto.nativeworker;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

public abstract class AbstractTestNativePlanValidation
        extends AbstractTestQueryFramework
{
    @Test
    public void testNestedLoopJoinPlainValidationFailure()
    {
        assertQueryFails(
                "SELECT EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) " +
                        "FROM lineitem l LIMIT 1", ".*Plan uses nested join loop which isn't supported.*");
    }
}
