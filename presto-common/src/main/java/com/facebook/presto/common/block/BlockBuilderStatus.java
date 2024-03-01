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
package com.facebook.presto.common.block;

import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class BlockBuilderStatus
{
    public static final int INSTANCE_SIZE = ClassLayout.parseClass(BlockBuilderStatus.class).instanceSize() + PageBuilderStatus.INSTANCE_SIZE;

    private final PageBuilderStatus pageBuilderStatus;

    private int currentSize;

    BlockBuilderStatus(PageBuilderStatus pageBuilderStatus)
    {
        this.pageBuilderStatus = requireNonNull(pageBuilderStatus, "pageBuilderStatus must not be null");
    }

    public int getMaxPageSizeInBytes()
    {
        return pageBuilderStatus.getMaxPageSizeInBytes();
    }

    public void addBytes(int bytes)
    {
        currentSize += bytes;
        pageBuilderStatus.addBytes(bytes);
    }

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder("BlockBuilderStatus{");
        buffer.append(", currentSize=").append(currentSize);
        buffer.append('}');
        return buffer.toString();
    }
}
