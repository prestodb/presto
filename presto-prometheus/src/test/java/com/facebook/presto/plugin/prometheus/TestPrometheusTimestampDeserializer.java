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
package com.facebook.presto.plugin.prometheus;

import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.time.ZonedDateTime;

import static org.testng.Assert.assertEquals;

public class TestPrometheusTimestampDeserializer
{
    @Test
    public void testDeserializeTimestamp()
    {
        ZonedDateTime now = ZonedDateTime.now();
        long nowEpochMillis = now.toInstant().toEpochMilli();
        String nowTimeStr = PrometheusSplitManager.decimalSecondString(nowEpochMillis);
        Timestamp nowTimestampActual = PrometheusTimestampDeserializer.decimalEpochTimestampToSQLTimestamp(nowTimeStr);
        assertEquals(nowTimestampActual, new Timestamp(now.toInstant().toEpochMilli()));
    }
}
