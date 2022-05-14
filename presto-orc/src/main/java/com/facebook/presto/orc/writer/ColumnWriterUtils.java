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
package com.facebook.presto.orc.writer;

import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.StreamCheckpoint;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.ValueOutputStream;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ColumnWriterUtils
{
    private ColumnWriterUtils()
    {
        // utils
    }

    /**
     * Build RowGroupIndex using column statistics and checkpoints.
     */
    @SafeVarargs
    public static List<RowGroupIndex> buildRowGroupIndexes(
            boolean compressed,
            List<ColumnStatistics> rowGroupColumnStatistics,
            Optional<List<? extends StreamCheckpoint>> prependCheckpoints,
            PresentOutputStream presentStream,
            ValueOutputStream<? extends StreamCheckpoint>... dataStreams)
    {
        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();

        List<List<? extends StreamCheckpoint>> dataCheckpoints = Arrays.stream(dataStreams)
                .map(ValueOutputStream::getCheckpoints)
                .collect(Collectors.toList());

        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            Optional<StreamCheckpoint> prependCheckpoint = prependCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            Optional<StreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));

            // prepend and present checkpoints always come first
            ImmutableList.Builder<Integer> positions = ImmutableList.builder();
            prependCheckpoint.ifPresent(checkpoint -> positions.addAll(checkpoint.toPositionList(compressed)));
            presentCheckpoint.ifPresent(checkpoint -> positions.addAll(checkpoint.toPositionList(compressed)));

            // add data checkpoints
            for (List<? extends StreamCheckpoint> dataCheckpoint : dataCheckpoints) {
                StreamCheckpoint streamCheckpoint = dataCheckpoint.get(groupId);
                positions.addAll(streamCheckpoint.toPositionList(compressed));
            }

            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            rowGroupIndexes.add(new RowGroupIndex(positions.build(), columnStatistics));
        }

        return rowGroupIndexes.build();
    }
}
