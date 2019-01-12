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
package io.prestosql.operator.project;

import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorSession;

public interface PageFilter
{
    boolean isDeterministic();

    InputChannels getInputChannels();

    SelectedPositions filter(ConnectorSession session, Page page);

    static SelectedPositions positionsArrayToSelectedPositions(boolean[] selectedPositions, int size)
    {
        int selectedCount = 0;
        for (int i = 0; i < size; i++) {
            boolean selectedPosition = selectedPositions[i];
            if (selectedPosition) {
                selectedCount++;
            }
        }

        if (selectedCount == 0 || selectedCount == size) {
            return SelectedPositions.positionsRange(0, selectedCount);
        }

        int[] positions = new int[selectedCount];
        int index = 0;
        for (int position = 0; position < size; position++) {
            if (selectedPositions[position]) {
                positions[index] = position;
                index++;
            }
        }
        return SelectedPositions.positionsList(positions, 0, selectedCount);
    }
}
