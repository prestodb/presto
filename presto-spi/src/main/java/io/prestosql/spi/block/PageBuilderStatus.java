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
package io.prestosql.spi.block;

public class PageBuilderStatus
{
    public static final int DEFAULT_MAX_PAGE_SIZE_IN_BYTES = 1024 * 1024;

    private final int maxPageSizeInBytes;

    private long currentSize;

    public PageBuilderStatus()
    {
        this(DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
    }

    public PageBuilderStatus(int maxPageSizeInBytes)
    {
        this.maxPageSizeInBytes = maxPageSizeInBytes;
    }

    public BlockBuilderStatus createBlockBuilderStatus()
    {
        return new BlockBuilderStatus(this);
    }

    public int getMaxPageSizeInBytes()
    {
        return maxPageSizeInBytes;
    }

    public boolean isEmpty()
    {
        return currentSize == 0;
    }

    public boolean isFull()
    {
        return currentSize >= maxPageSizeInBytes;
    }

    void addBytes(int bytes)
    {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes cannot be negative");
        }
        currentSize += bytes;
    }

    public long getSizeInBytes()
    {
        return currentSize;
    }

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder("BlockBuilderStatus{");
        buffer.append("maxSizeInBytes=").append(maxPageSizeInBytes);
        buffer.append(", currentSize=").append(currentSize);
        buffer.append('}');
        return buffer.toString();
    }
}
