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
package com.facebook.presto.cli;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ColumnAligner
{
    private final int leftMargin;
    private final List<String> values;

    private int currentLength;

    public ColumnAligner(String header, int minimalLength, int leftMargin)
    {
        requireNonNull(header, "header is null");
        this.values = new ArrayList<>();
        this.values.add(header);
        if (header.length() >= minimalLength) {
            this.currentLength = header.length();
        }
        else {
            this.currentLength = minimalLength;
        }
        this.leftMargin = leftMargin;
    }

    public void feedValue(Object value)
    {
        requireNonNull(value, "value is null");

        String valueString = value.toString();
        if (currentLength < valueString.length()) {
            currentLength = valueString.length();
        }
        values.add(valueString);
    }

    public List<String> buildColumn()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String value : values) {
            int gapLength = currentLength - value.length();
            builder.add(Strings.repeat(" ", gapLength + leftMargin) + value);
        }
        return builder.build();
    }
}
