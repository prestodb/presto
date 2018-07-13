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
package com.facebook.presto.pulsar;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

public class PulsarColumnMetadata extends ColumnMetadata{

    private boolean isInternal;
    private Integer positionIndex;
    // need this because presto ColumnMetadata saves name in lowercase
    private String nameWithCase;

    public PulsarColumnMetadata(String name, Type type, String comment, String extraInfo,
                                boolean hidden, boolean isInternal, Integer positionIndex) {
        super(name, type, comment, extraInfo, hidden);
        this.nameWithCase = name;
        this.isInternal = isInternal;
        this.positionIndex = positionIndex;
    }

    public String getNameWithCase() {
        return nameWithCase;
    }

    public boolean isInternal() {
        return isInternal;
    }

    public int getPositionIndex() {
        return positionIndex;
    }


    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("PulsarColumnMetadata{");
        sb.append("name='").append(getName()).append('\'');
        sb.append(", type=").append(getType());
        if (getComment() != null) {
            sb.append(", comment='").append(getComment()).append('\'');
        }
        if (getExtraInfo() != null) {
            sb.append(", extraInfo='").append(getExtraInfo()).append('\'');
        }
        if (isHidden()) {
            sb.append(", hidden");
        }
        if (isInternal()) {
            sb.append(", internal");
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PulsarColumnMetadata that = (PulsarColumnMetadata) o;

        return isInternal == that.isInternal;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (isInternal ? 1 : 0);
        return result;
    }
}
