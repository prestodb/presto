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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestMaterializedViewScan
{
    @Test
    public void testGetChildren()
    {
        QualifiedName mvName = QualifiedName.of("catalog", "schema", "my_mv");
        Table dataTable = new Table(QualifiedName.of("catalog", "schema", "mv_storage"));
        QuerySpecification viewQuery = new QuerySpecification(
                new Select(false, ImmutableList.of(new AllColumns())),
                Optional.of(new Table(QualifiedName.of("catalog", "schema", "base_table"))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        Query query = new Query(Optional.empty(), viewQuery, Optional.empty(), Optional.empty(), Optional.empty());

        MaterializedViewScan mvScan = new MaterializedViewScan(mvName, dataTable, query);

        assertEquals(mvScan.getChildren(), ImmutableList.of(dataTable, query));
        assertEquals(mvScan.getMaterializedViewName(), mvName);
        assertEquals(mvScan.getDataTable(), dataTable);
        assertEquals(mvScan.getViewQuery(), query);
    }

    @Test
    public void testEquals()
    {
        QualifiedName mvName1 = QualifiedName.of("catalog", "schema", "my_mv");
        QualifiedName mvName2 = QualifiedName.of("catalog", "schema", "my_mv");
        QualifiedName mvNameDifferent = QualifiedName.of("catalog", "schema", "different_mv");

        Table dataTable1 = new Table(QualifiedName.of("catalog", "schema", "mv_storage"));
        Table dataTable2 = new Table(QualifiedName.of("catalog", "schema", "mv_storage"));
        Table dataTableDifferent = new Table(QualifiedName.of("catalog", "schema", "other_table"));

        QuerySpecification viewQuerySpec = new QuerySpecification(
                new Select(false, ImmutableList.of(new AllColumns())),
                Optional.of(new Table(QualifiedName.of("catalog", "schema", "base_table"))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        Query viewQuery1 = new Query(Optional.empty(), viewQuerySpec, Optional.empty(), Optional.empty(), Optional.empty());
        Query viewQuery2 = new Query(Optional.empty(), viewQuerySpec, Optional.empty(), Optional.empty(), Optional.empty());

        MaterializedViewScan mvScan1 = new MaterializedViewScan(mvName1, dataTable1, viewQuery1);
        MaterializedViewScan mvScan2 = new MaterializedViewScan(mvName2, dataTable2, viewQuery2);
        MaterializedViewScan mvScanDifferent = new MaterializedViewScan(mvNameDifferent, dataTableDifferent, viewQuery1);

        assertEquals(mvScan1, mvScan2);
        assertEquals(mvScan1.hashCode(), mvScan2.hashCode());
        assertNotEquals(mvScan1, mvScanDifferent);
    }

    @Test
    public void testVisitor()
    {
        QualifiedName mvName = QualifiedName.of("catalog", "schema", "my_mv");
        Table dataTable = new Table(QualifiedName.of("catalog", "schema", "mv_storage"));
        QuerySpecification viewQuerySpec = new QuerySpecification(
                new Select(false, ImmutableList.of(new AllColumns())),
                Optional.of(new Table(QualifiedName.of("catalog", "schema", "base_table"))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        Query viewQuery = new Query(Optional.empty(), viewQuerySpec, Optional.empty(), Optional.empty(), Optional.empty());

        MaterializedViewScan mvScan = new MaterializedViewScan(mvName, dataTable, viewQuery);

        String result = mvScan.accept(new AstVisitor<String, Void>()
        {
            @Override
            protected String visitMaterializedViewScan(MaterializedViewScan node, Void context)
            {
                return "MaterializedViewScan";
            }
        }, null);

        assertEquals(result, "MaterializedViewScan");
    }
}
