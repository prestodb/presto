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
package com.facebook.presto.execution;

import com.google.common.base.Optional;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AddPartition;
import com.facebook.presto.sql.tree.PartitionItem;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;

public class AddPartitionTask
        implements DataDefinitionTask<AddPartition>
{
    @Override
    public void execute(AddPartition statement, ConnectorSession session, Metadata metadata)
    {
        QualifiedTableName tableName = createQualifiedTableName(session, statement.getSource());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        List<PartitionItem> partitionItemList = ((AddPartition) statement).getPartitionItemList();
        List<String> partitionValues = new ArrayList<String>();
        Iterator<PartitionItem> itr = partitionItemList.iterator();
        while (itr.hasNext()) {
            PartitionItem partitionItem = itr.next();
            partitionValues.add(partitionItem.getValue());
        }
        metadata.addPartition(tableHandle.get(), partitionValues);
    }
}
