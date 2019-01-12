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
package io.prestosql.plugin.ml;

import io.prestosql.plugin.ml.type.ModelType;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_parameter;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;

import static io.prestosql.plugin.ml.type.RegressorType.REGRESSOR;
import static java.util.Objects.requireNonNull;

public class SvmRegressor
        extends AbstractSvmModel
        implements Regressor
{
    public SvmRegressor()
    {
        this(LibSvmUtils.parseParameters(""));
    }

    public SvmRegressor(svm_parameter params)
    {
        super(params);
    }

    private SvmRegressor(svm_model model)
    {
        super(model);
    }

    public static SvmRegressor deserialize(byte[] modelData)
    {
        // TODO do something with the hyperparameters
        try {
            svm_model model = svm.svm_load_model(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(modelData))));
            return new SvmRegressor(model);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public double regress(FeatureVector features)
    {
        requireNonNull(model, "model is null");
        return svm.svm_predict(model, toSvmNodes(features));
    }

    @Override
    public ModelType getType()
    {
        return REGRESSOR;
    }

    @Override
    protected int getLibsvmType()
    {
        return svm_parameter.NU_SVR;
    }
}
