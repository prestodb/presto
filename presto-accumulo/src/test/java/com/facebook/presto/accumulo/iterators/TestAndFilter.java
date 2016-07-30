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
package com.facebook.presto.accumulo.iterators;

import com.facebook.presto.accumulo.iterators.SingleColumnValueFilter.CompareOp;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.accumulo.iterators.SingleColumnValueFilter.getProperties;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAndFilter
{
    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();

    private static byte[] encode(Type type, Object v)
    {
        return SERIALIZER.encode(type, v);
    }

    private final IteratorSetting cq1gt3 = new IteratorSetting(1, "1", SingleColumnValueFilter.class, getProperties("cf1", "cq1", CompareOp.GREATER, encode(BigintType.BIGINT, 3L)));
    private final IteratorSetting cq1lt10 = new IteratorSetting(2, "2", SingleColumnValueFilter.class, getProperties("cf1", "cq1", CompareOp.LESS, encode(BigintType.BIGINT, 10L)));
    private final IteratorSetting cq2lt10 = new IteratorSetting(2, "2", SingleColumnValueFilter.class, getProperties("cf1", "cq2", CompareOp.LESS, encode(BigintType.BIGINT, 10L)));

    @Test
    public void testSameColumn()
            throws IOException
    {
        IteratorSetting settings = AndFilter.andFilters(3, cq1gt3, cq1lt10);

        TestKeyValueIterator iter = new TestKeyValueIterator(key("row1", "cf1", "cq1"), value(BigintType.BIGINT, 4L));

        AndFilter filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        assertTrue(filter.acceptRow(iter));

        iter.clear().add(key("row1", "cf1", "cq1"), value(BigintType.BIGINT, 3L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        assertFalse(filter.acceptRow(iter));

        iter.clear().add(key("row1", "cf1", "cq1"), value(BigintType.BIGINT, 10L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        assertFalse(filter.acceptRow(iter));
    }

    @Test
    public void testDifferentColumns()
            throws IOException
    {
        IteratorSetting settings = AndFilter.andFilters(1, cq1gt3, cq2lt10);

        TestKeyValueIterator iter = new TestKeyValueIterator();
        iter.add(key("row1", "cf1", "cq1"), value(BigintType.BIGINT, 4L));
        iter.add(key("row1", "cf1", "cq2"), value(BigintType.BIGINT, 9L));

        AndFilter filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        assertTrue(filter.acceptRow(iter));

        iter.clear()
                .add(key("row1", "cf1", "cq1"), value(BigintType.BIGINT, 10L))
                .add(key("row1", "cf1", "cq2"), value(BigintType.BIGINT, 8L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        assertTrue(filter.acceptRow(iter));

        iter.clear()
                .add(key("row1", "cf1", "cq1"), value(BigintType.BIGINT, 3L))
                .add(key("row1", "cf1", "cq2"), value(BigintType.BIGINT, 8L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        assertFalse(filter.acceptRow(iter));

        iter.clear()
                .add(key("row1", "cf1", "cq1"), value(BigintType.BIGINT, 4L))
                .add(key("row1", "cf1", "cq2"), value(BigintType.BIGINT, 10L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        assertFalse(filter.acceptRow(iter));
    }

    @Test
    public void testNullColumn()
            throws IOException
    {
        IteratorSetting settings = AndFilter.andFilters(1, cq1gt3, cq2lt10);
        TestKeyValueIterator iter = new TestKeyValueIterator(key("row1", "cf1", "cq1"), value(BigintType.BIGINT, 4L));

        AndFilter filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        assertFalse(filter.acceptRow(iter));

        iter.clear().add(key("row1", "cf1", "cq2"), value(BigintType.BIGINT, 10L));
        filter = new AndFilter();
        filter.init(iter, settings.getOptions(), null);
        assertFalse(filter.acceptRow(iter));
    }

    private static Key key(String r, String f, String q)
    {
        return new Key(r, f, q);
    }

    private static Value value(Type t, Object o)
    {
        return new Value(encode(t, o));
    }
}
