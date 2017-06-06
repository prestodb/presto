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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DeferredSymbolReference;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestExpressionSerialization
{
    private JsonCodecFactory jsonCodecFactory;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        ObjectMapperProvider mapperProvider = new ObjectMapperProvider();
        mapperProvider.setJsonSerializers(ImmutableMap.of(Expression.class, new Serialization.ExpressionSerializer()));
        mapperProvider.setJsonDeserializers(ImmutableMap.of(Expression.class, new Serialization.ExpressionDeserializer(new SqlParser())));
        jsonCodecFactory = new JsonCodecFactory(mapperProvider);
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        jsonCodecFactory = null;
    }

    @Test
    public void testDeferredSymbolReference()
            throws Exception
    {
        assertJsonSerialization(new DeferredSymbolReference("source", "name"));
    }

    @Test
    public void testSymbolReference()
            throws Exception
    {
        assertJsonSerialization(new SymbolReference("symbol"));
    }

    private void assertJsonSerialization(Expression expression)
    {
        JsonCodec<Expression> expressionCodec = jsonCodecFactory.jsonCodec(Expression.class);
        assertEquals(expressionCodec.fromJson(expressionCodec.toJson(expression)), expression);
        JsonCodec<ExpressionHolder> expressionHolderCodec = jsonCodecFactory.jsonCodec(ExpressionHolder.class);
        assertEquals(expressionHolderCodec.fromJson(expressionHolderCodec.toJson(new ExpressionHolder(expression))), new ExpressionHolder(expression));
        JsonCodec<List<Expression>> expressionListCodec = jsonCodecFactory.listJsonCodec(Expression.class);
        assertEquals(expressionListCodec.fromJson(expressionListCodec.toJson(ImmutableList.of(expression))), ImmutableList.of(expression));
        assertEquals(expressionListCodec.fromJson(expressionListCodec.toJson(ImmutableList.of(expression, expression))), ImmutableList.of(expression, expression));
    }

    public static class ExpressionHolder
    {
        private final Expression expression;

        @JsonCreator
        public ExpressionHolder(@JsonProperty("expression") Expression expression)
        {
            this.expression = requireNonNull(expression, "expression is null");
        }

        @JsonProperty("expression")
        public Expression getExpression()
        {
            return expression;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExpressionHolder that = (ExpressionHolder) o;
            return Objects.equals(expression, that.expression);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expression);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("expression", expression)
                    .toString();
        }
    }
}
