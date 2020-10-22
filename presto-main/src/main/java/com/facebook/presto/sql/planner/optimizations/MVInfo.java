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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.QualifiedObjectName;

import java.util.HashMap;
import java.util.Map;

public class MVInfo
{
    private QualifiedObjectName mvObject;
    private QualifiedObjectName baseObject;
    private Map<String, String> expressionToMVColumnName;

    public MVInfo()
    {
    }

    public MVInfo(
            QualifiedObjectName baseObject,
            QualifiedObjectName mvObject,
            HashMap<String, String> expressionToMVColumnName)
    {
        this.mvObject = mvObject;
        this.baseObject = baseObject;
        this.expressionToMVColumnName = expressionToMVColumnName;
    }

    public QualifiedObjectName getMvObject()
    {
        return mvObject;
    }

    public void setMvObject(QualifiedObjectName mvObject)
    {
        this.mvObject = mvObject;
    }

    public QualifiedObjectName getBaseObject()
    {
        return baseObject;
    }

    public void setBaseObject(QualifiedObjectName baseObject)
    {
        this.baseObject = baseObject;
    }

    public Map<String, String> getExpressionToMVColumnName()
    {
        return expressionToMVColumnName;
    }

    public void setExpressionToMVColumnName(Map<String, String> expressionToMVColumnName)
    {
        this.expressionToMVColumnName = expressionToMVColumnName;
    }
}
