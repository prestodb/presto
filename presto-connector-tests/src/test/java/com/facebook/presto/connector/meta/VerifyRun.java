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
package com.facebook.presto.connector.meta;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.TestInfo;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VerifyRun
        implements BaseSPITest
{
    private final Set<String> shouldRunTestNames;
    private Set<String> didRunTestNames;

    protected VerifyRun()
    {
        this(ImmutableSet.of());
    }

    protected VerifyRun(Set<String> shouldRunTestNames)
    {
        this.shouldRunTestNames = shouldRunTestNames;
        this.didRunTestNames = new HashSet<>();
    }

    @Override
    public void ran(TestInfo testInfo)
    {
        didRunTestNames.add(testInfo.getTestMethod().get().getName());
    }

    @Override
    public void validate()
    {
        assertEquals(shouldRunTestNames, didRunTestNames);
    }
}
