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
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;

import static com.google.common.base.Preconditions.checkState;

public class TwoChannelJoinProbe
        implements JoinProbe
{
    public static class TwoChannelJoinProbeFactory
            implements JoinProbeFactory
    {
        @Override
        public JoinProbe createJoinProbe(JoinHash hash, Page page)
        {
            return new TwoChannelJoinProbe(hash, page);
        }
    }

    private final JoinHash hash;
    private final BlockCursor cursorA;
    private final BlockCursor cursorB;
    private final BlockCursor probeCursorA;
    private final BlockCursor probeCursorB;
    private final BlockCursor[] probeCursors;

    public TwoChannelJoinProbe(JoinHash hash, Page page)
    {
        this.hash = hash;
        this.cursorA = page.getBlock(0).cursor();
        this.cursorB = page.getBlock(1).cursor();
        this.probeCursorA = cursorA;
        this.probeCursorB = cursorB;
        this.probeCursors = new BlockCursor[2];
        probeCursors[0] = probeCursorA;
        probeCursors[1] = probeCursorB;
    }

    @Override
    public int getChannelCount()
    {
        return 2;
    }

    @Override
    public void appendTo(PageBuilder pageBuilder)
    {
        cursorA.appendTo(pageBuilder.getBlockBuilder(0));
        cursorA.appendTo(pageBuilder.getBlockBuilder(1));
    }

    @Override
    public boolean advanceNextPosition()
    {
        boolean advanced = cursorA.advanceNextPosition();
        checkState(advanced == cursorB.advanceNextPosition());
        return advanced;
    }

    @Override
    public int getCurrentJoinPosition()
    {
        if (currentRowContainsNull()) {
            return -1;
        }
        return hash.getJoinPosition(probeCursors);
    }

    private boolean currentRowContainsNull()
    {
        if (probeCursorA.isNull()) {
            return true;
        }
        if (probeCursorB.isNull()) {
            return true;
        }
        return false;
    }
}
