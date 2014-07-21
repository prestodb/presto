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
package com.facebook.presto.ml;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Dataset
{
    private final List<FeatureVector> datapoints;
    private final List<Double> labels;

    public Dataset(List<Double> labels, List<FeatureVector> datapoints)
    {
        checkNotNull(datapoints, "datapoints is null");
        checkNotNull(labels, "labels is null");
        checkArgument(datapoints.size() == labels.size(), "datapoints and labels have different sizes");
        this.labels = ImmutableList.copyOf(labels);
        this.datapoints = ImmutableList.copyOf(datapoints);
    }

    public List<FeatureVector> getDatapoints()
    {
        return datapoints;
    }

    public List<Double> getLabels()
    {
        return labels;
    }
}
