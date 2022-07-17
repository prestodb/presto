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
package com.facebook.presto.redis.decoder.hash;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.redis.RedisColumnHandle;
import com.facebook.presto.testing.assertions.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimeType.TIME;

@Test
public class TestISO8601HashRedisFieldDecoder
{
    private ISO8601HashRedisFieldDecoder iso8601HashRedisFieldDecoder = new ISO8601HashRedisFieldDecoder();

    private RedisColumnHandle createTypedColumnHandle(Type t)
    {
        return new RedisColumnHandle("", 0, "", t, "", "", "", false, false, false);
    }

    @Test
    public void testDateToDate()
    {
        FieldValueProvider provider = iso8601HashRedisFieldDecoder.decode("1970-01-15", createTypedColumnHandle(DATE));
        Assert.assertEquals(provider.getLong(), 14);
    }

    @Test
    public void testDateToTime()
    {
        FieldValueProvider provider = iso8601HashRedisFieldDecoder.decode("1970-01-20", createTypedColumnHandle(TIME));
        Assert.assertEquals(provider.getLong(), 19 * 24 * 60 * 60 * 1000);
    }

    @Test
    public void testTimeToDate()
    {
        FieldValueProvider provider = iso8601HashRedisFieldDecoder.decode("1970-01-25 10:30:00", createTypedColumnHandle(DATE));
        Assert.assertEquals(provider.getLong(), 24);
    }

    @Test
    public void testTimeToTime()
    {
        FieldValueProvider provider = iso8601HashRedisFieldDecoder.decode("1970-01-12 07:15:00", createTypedColumnHandle(TIME));
        Assert.assertEquals(provider.getLong(), (11 * 24 + 7.25) * 60 * 60 * 1000);
    }
}
