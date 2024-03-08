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
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * This wrapper class is added to avoid adding special classes and simplifying
 * the presto protocol generation for Prestissimo
 */
public enum FileContent
{
    DATA(0),
    POSITION_DELETES(1),
    EQUALITY_DELETES(2);

    private final int id;

    FileContent(int id)
    {
        this.id = id;
    }

    public int id()
    {
        return id;
    }

    public static FileContent fromIcebergFileContent(org.apache.iceberg.FileContent fileContent)
    {
        FileContent prestoFileContent;
        switch (fileContent) {
            case DATA:
                prestoFileContent = DATA;
                break;
            case POSITION_DELETES:
                prestoFileContent = POSITION_DELETES;
                break;
            case EQUALITY_DELETES:
                prestoFileContent = EQUALITY_DELETES;
                break;
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported iceberg content type: " + fileContent);
        }

        return prestoFileContent;
    }
}
