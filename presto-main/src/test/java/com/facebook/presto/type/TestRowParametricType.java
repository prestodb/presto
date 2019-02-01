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
package com.facebook.presto.type;

import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.RowFieldName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestRowParametricType
{
    @Test
    public void testTypeSignatureRoundTrip()
    {
        TypeManager typeManager = new TypeRegistry();
        TypeSignature typeSignature = new TypeSignature(
                ROW,
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("col1")), new TypeSignature(BIGINT))),
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("col2")), new TypeSignature(DOUBLE))));
        List<TypeParameter> parameters = typeSignature.getParameters().stream()
                .map(parameter -> TypeParameter.of(parameter, typeManager))
                .collect(Collectors.toList());
        Type rowType = RowParametricType.ROW.createType(typeManager, parameters);

        assertEquals(rowType.getTypeSignature(), typeSignature);
    }
}
