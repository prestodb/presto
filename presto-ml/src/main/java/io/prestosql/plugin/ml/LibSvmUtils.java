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

import com.google.common.base.Splitter;
import io.prestosql.spi.PrestoException;
import libsvm.svm_parameter;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class LibSvmUtils
{
    private LibSvmUtils()
    {
    }

    public static svm_parameter parseParameters(String paramString)
    {
        svm_parameter params = new svm_parameter();
        params.kernel_type = svm_parameter.LINEAR;
        params.degree = 3;
        params.gamma = 0;
        params.coef0 = 0;
        params.nu = 0.5;
        params.cache_size = 100;
        params.C = 1;
        params.eps = 0.1;
        params.p = 0.1;
        params.shrinking = 1;
        params.probability = 0;
        params.nr_weight = 0;
        params.weight_label = new int[0];
        params.weight = new double[0];

        for (String split : Splitter.on(',').omitEmptyStrings().split(paramString)) {
            String[] pair = split.split("=");
            checkArgument(pair.length == 2, "Invalid hyperparameters string for libsvm");
            String value = pair[1].trim();

            String key = pair[0].trim();
            switch (key) {
                case "kernel":
                    params.kernel_type = parseKernelType(value);
                    break;
                case "degree":
                    params.degree = Integer.parseInt(value);
                    break;
                case "gamma":
                    params.gamma = Double.parseDouble(value);
                    break;
                case "coef0":
                    params.coef0 = Double.parseDouble(value);
                    break;
                case "C":
                    params.C = Double.parseDouble(value);
                    break;
                case "nu":
                    params.nu = Double.parseDouble(value);
                    break;
                case "eps":
                    params.eps = Double.parseDouble(value);
                    break;
                default:
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unknown parameter %s", pair[0]));
            }
        }

        return params;
    }

    private static int parseKernelType(String value)
    {
        switch (value.toLowerCase(ENGLISH)) {
            case "linear":
                return svm_parameter.LINEAR;
            case "poly":
                return svm_parameter.POLY;
            case "rbf":
                return svm_parameter.RBF;
            case "sigmoid":
                return svm_parameter.SIGMOID;
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unknown kernel type %s", value));
        }
    }
}
