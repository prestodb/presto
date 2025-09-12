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

import React from 'react';
import DataTable from 'react-data-table-component';

export const CUSTOM_STYLES = {
    headCells: {
        style: {
            padding: '2px', // override the cell padding for head cells
            fontSize: '15px',
        },
    },
    cells: {
        style: {
            padding: '2px', // override the cell padding for data cells
            fontSize: '15px',
        },
    },
};

// Show query results in a table view
export function QueryResults({ results }) {

    const getColumns = () => {
        return results.columns.map((row, index) => {
            let column = {
                name: row.name,
            };
            column.selector = row.type === 'bigint' ? row => row[index]?.toString() ?? 'NULL' : row => row[index];
            return column;
        });
    };

    return (
        <>
            <div className="row">
                <div className='col-6'>
                    <h3>Results</h3>
                </div>
                <div className="col-6 mt-2">
                    {results.queryId &&
                        <a style={{ display: 'block'}}
                            href={"query.html?" + results.queryId} target="_blank"
                            data-bs-toggle="tooltip" data-trigger="hover" title="Query ID"
                        >
                            {results.queryId}
                        </a>}
                </div>
            </div>
            <div className="row"><hr className="h3-hr"/></div>
            {results.error && <div className="row">
                <div className="alert alert-danger" role="alert" style={{ background: '#f2dede',border: '#ebccd1', color: '#a94442', fontSize:'18px'}}>
                    <h4 className="text-center" style ={{fontSize:'18px'}}>
                        {results.error.message}</h4>
                </div>
            </div>}
            {results.data && <div className="row">
                <DataTable columns={getColumns()}
                    data={results.data}
                    theme='dark'
                    customStyles={CUSTOM_STYLES}
                    striped={true}
                    pagination />
            </div>}
        </>
    );
}
