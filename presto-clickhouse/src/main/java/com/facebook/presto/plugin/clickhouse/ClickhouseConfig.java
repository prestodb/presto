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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.math.RoundingMode;

public class ClickhouseConfig
{
    private int varcharMaxSize = 4000;

    private int numberDefaultScale = 10;

    private RoundingMode numberRoundingMode = RoundingMode.HALF_UP;

    @Min(0L)
    @Max(38L)
    public int getNumberDefaultScale()
    {
        return this.numberDefaultScale;
    }

    @Config("clickhouse.number.default-scale")
    public ClickhouseConfig setNumberDefaultScale(int numberDefaultScale)
    {
        this.numberDefaultScale = numberDefaultScale;
        return this;
    }

    @NotNull
    public RoundingMode getNumberRoundingMode()
    {
        return this.numberRoundingMode;
    }

    @Config("clickhouse.number.rounding-mode")
    public ClickhouseConfig setNumberRoundingMode(RoundingMode numberRoundingMode)
    {
        this.numberRoundingMode = numberRoundingMode;
        return this;
    }

    @Min(4000L)
    public int getVarcharMaxSize()
    {
        return this.varcharMaxSize;
    }

    @Config("clickhouse.varchar.max-size")
    public ClickhouseConfig setVarcharMaxSize(int varcharMaxSize)
    {
        this.varcharMaxSize = varcharMaxSize;
        return this;
    }
}
