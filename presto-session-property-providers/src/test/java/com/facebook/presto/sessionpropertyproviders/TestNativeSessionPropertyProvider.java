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
package com.facebook.presto.sessionpropertyproviders;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static org.testng.Assert.assertEquals;

public class TestNativeSessionPropertyProvider
{
    // This is only for testing purpose.
    private static class TestNodeManager
            implements NodeManager
    {
        @Override
        public Set<Node> getAllNodes()
        {
            return null;
        }

        @Override
        public Set<Node> getWorkerNodes()
        {
            return null;
        }

        @Override
        public Node getCurrentNode()
        {
            return null;
        }

        @Override
        public String getEnvironment()
        {
            return null;
        }
    }

    private static class TestTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            switch (signature.getBase()) {
                case "integer":
                    return INTEGER;
                case "boolean":
                    return BOOLEAN;
                case "varchar":
                    return VARCHAR;
                case "double":
                    return DOUBLE;
                default:
                    return null;
            }
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return null;
        }

        @Override
        public boolean canCoerce(Type actualType, Type expectedType)
        {
            return false;
        }
    }

    NativeSystemSessionPropertyProvider propertyProvider = new NativeSystemSessionPropertyProvider(new TestNodeManager(), new TestTypeManager());

    @Test
    public void testDeserializeSessionProperties()
            throws URISyntaxException
    {
        String responseBody =
                "[" +
                        "{\"name\":\"sample1\",\"description\":\"Sample description 1\",\"typeSignature\":\"integer\",\"defaultValue\":\"100\",\"hidden\":true}," +
                        "{\"name\":\"sample2\",\"description\":\"Sample description 2\",\"typeSignature\":\"boolean\",\"defaultValue\":\"true\",\"hidden\":false}," +
                        "{\"name\":\"sample3\",\"description\":\"Sample description 3\",\"typeSignature\":\"varchar\",\"defaultValue\":\"N/A\",\"hidden\":true}," +
                        "{\"name\":\"sample4\",\"description\":\"Sample description 4\",\"typeSignature\":\"double\",\"defaultValue\":\"3.14\",\"hidden\":false}" +
                        "]";

        // Correct object creation to match responseBody
        PropertyMetadata<Integer> s1 = integerProperty("sample1", "Sample description 1", 100, true);
        PropertyMetadata<Boolean> s2 = booleanProperty("sample2", "Sample description 2", true, false);
        PropertyMetadata<String> s3 = stringProperty("sample3", "Sample description 3", "N/A", true);
        PropertyMetadata<Double> s4 = doubleProperty("sample4", "Sample description 4", 3.14, false);

        List<PropertyMetadata<?>> expected = ImmutableList.of(s1, s2, s3, s4);
        List<PropertyMetadata<?>> decoded = propertyProvider.deserializeSessionProperties(responseBody);

        assertEquals(decoded.toArray(), expected.toArray());
    }
}
