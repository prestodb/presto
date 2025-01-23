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
package com.facebook.presto.sql;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestNodeUtils
{
    @Test
    public void testMapFromProperties()
    {
        List<Property> propertyList = new ArrayList<>();
        propertyList.add(new Property(new Identifier("a"), new StringLiteral("apple")));
        propertyList.add(new Property(new Identifier("b"), new StringLiteral("ball")));
        Map<String, Expression> stringExpressionMap = NodeUtils.mapFromProperties(propertyList);
        assertEquals(2, stringExpressionMap.size());
        assertEquals(new StringLiteral("apple"), stringExpressionMap.get("a"));
        assertEquals(new StringLiteral("ball"), stringExpressionMap.get("b"));
    }

    @Test
    public void testDuplicateKeyFromProperties()
    {
        List<Property> propertyList = new ArrayList<>();
        propertyList.add(new Property(new Identifier("partitioned_by"), new ArrayConstructor(ImmutableList.of(new StringLiteral("ds")))));
        propertyList.add(new Property(new Identifier("partitioned_by"), new ArrayConstructor(ImmutableList.of(new StringLiteral("source")))));

        try {
            NodeUtils.mapFromProperties(propertyList);
            fail("PrestoException not thrown for duplicate properties");
        }
        catch (Exception e) {
            assertTrue(e instanceof PrestoException, format("Expected PrestoException but found %s", e));
            PrestoException prestoException = (PrestoException) e;
            assertEquals(prestoException.getErrorCode(), SYNTAX_ERROR.toErrorCode());
            assertEquals(prestoException.getMessage(), "Duplicate property found: partitioned_by=ARRAY['ds'] and partitioned_by=ARRAY['source']");
        }
    }
}
