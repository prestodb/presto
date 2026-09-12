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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTypeConverter
{
    private final TypeManager typeManager = createTestFunctionAndTypeManager();

    @Test
    public void testVariantMapsToJson()
    {
        assertEquals(toPrestoType(Types.VariantType.get(), typeManager), JsonType.JSON);
    }

    /**
     * Iceberg VARIANT is a primitive type (not nested), so ColumnIdentity must
     * classify it as PRIMITIVE with no children — matching the behaviour of
     * all other leaf Iceberg types such as STRING or LONG.
     */
    @Test
    public void testVariantColumnIdentityIsPrimitive()
    {
        ColumnIdentity identity = ColumnIdentity.createColumnIdentity("data", 1, Types.VariantType.get());
        assertEquals(identity.getTypeCategory(), PRIMITIVE);
        assertTrue(identity.getChildren().isEmpty());
    }

    /**
     * A schema containing a VARIANT column must round-trip through
     * {@link TypeConverter#toPrestoType} without throwing. The VARIANT column
     * must surface as a JSON column.
     */
    @Test
    public void testSchemaWithVariantColumn()
    {
        Schema schema = new Schema(ImmutableList.of(
                optional(1, "id", Types.IntegerType.get()),
                optional(2, "data", Types.VariantType.get())));

        assertEquals(toPrestoType(schema.findType("id"), typeManager).getTypeSignature().getBase(), "integer");
        assertEquals(toPrestoType(schema.findType("data"), typeManager), JsonType.JSON);
    }
}
