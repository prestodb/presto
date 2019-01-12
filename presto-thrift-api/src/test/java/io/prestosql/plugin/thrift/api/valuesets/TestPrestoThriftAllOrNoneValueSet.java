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
package io.prestosql.plugin.thrift.api.valuesets;

import io.prestosql.spi.predicate.ValueSet;
import org.testng.annotations.Test;

import static io.prestosql.plugin.thrift.api.valuesets.PrestoThriftValueSet.fromValueSet;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPrestoThriftAllOrNoneValueSet
{
    @Test
    public void testFromValueSetAll()
    {
        PrestoThriftValueSet thriftValueSet = fromValueSet(ValueSet.all(HYPER_LOG_LOG));
        assertNotNull(thriftValueSet.getAllOrNoneValueSet());
        assertTrue(thriftValueSet.getAllOrNoneValueSet().isAll());
    }

    @Test
    public void testFromValueSetNone()
    {
        PrestoThriftValueSet thriftValueSet = fromValueSet(ValueSet.none(HYPER_LOG_LOG));
        assertNotNull(thriftValueSet.getAllOrNoneValueSet());
        assertFalse(thriftValueSet.getAllOrNoneValueSet().isAll());
    }
}
