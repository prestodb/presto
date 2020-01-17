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
package com.facebook.presto.spi;

public enum NodeState
{
    ACTIVE(1),
    INACTIVE(2),
    SHUTTING_DOWN(3);

    private final int value;

    NodeState(int value)
    {
        this.value = value;
    }

    /**
     * Recover NodeState from the ordinal.
     * In general, ThriftEnum is the right annotation to use.
     * But given the class is in SPI, use the following workaround.
     */
    public static NodeState valueOf(int value)
    {
        for (NodeState nodeState : values()) {
            if (nodeState.getValue() == value) {
                return nodeState;
            }
        }
        throw new IllegalArgumentException("Invalid NodeState value: " + value);
    }

    // the value will be used for SerDe like thrift
    public int getValue()
    {
        return value;
    }
}
