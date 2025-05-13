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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.VIEW_NOT_FOUND;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class MetadataHandleTest
{
    private MetadataHandle metadataHandle;

    @BeforeMethod
    public void setUp()
    {
        metadataHandle = new MetadataHandle();
        ViewDefinition viewDefinition = new ViewDefinition(
                "select a from t1",
                Optional.of("tpch"),
                Optional.of("s1"),
                ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                Optional.of("user"),
                false);
        metadataHandle.addViewDefinition(QualifiedObjectName.valueOf("tpch.s1.t1"), Futures.immediateFuture(Optional.of(viewDefinition)));
    }

    @Test
    public void testGetViewDefinition()
    {
        Optional<ViewDefinition> viewDefinitionOptional = metadataHandle.getViewDefinition(QualifiedObjectName.valueOf("tpch.s1.t1"));
        assertTrue(viewDefinitionOptional.isPresent());
        ViewDefinition viewDefinition = viewDefinitionOptional.get();
        assertEquals("tpch", viewDefinition.getCatalog().get());
        assertEquals("s1", viewDefinition.getSchema().get());
        assertEquals("user", viewDefinition.getOwner().get());
        assertEquals("select a from t1", viewDefinition.getOriginalSql());
        assertFalse(viewDefinition.isRunAsInvoker());
    }

    @Test
    public void testGetViewDefinitionNotPresent()
    {
        try {
            metadataHandle.getViewDefinition(QualifiedObjectName.valueOf("tpch.s1.t2"));
            fail("PrestoException not thrown for invalid view");
        }
        catch (Exception e) {
            assertTrue(e instanceof PrestoException);
            PrestoException prestoException = (PrestoException) e;
            assertEquals(prestoException.getErrorCode(), VIEW_NOT_FOUND.toErrorCode());
            assertEquals(prestoException.getMessage(), "View tpch.s1.t2 not found, the available view names are: [tpch.s1.t1]");
        }
    }
}
