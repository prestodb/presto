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
import { clsx } from 'clsx';
import { CUSTOM_STYLES } from './QueryResults.jsx';
import { createClient } from './SQLInput.jsx';

const NameFilter = ({ filterText, onFilter, onClear }) => (
    <div className="input-group">
        <input
            type="text" className="form-control"
            id="search"
            placeholder="Filter By Name"
            aria-label="Search Input"
            value={filterText}
            onChange={onFilter} />
        <span className="input-group-btn">
            <button className="btn btn-default" type="button" onClick={onClear}>X</button>
        </span>
    </div>
);

const EditableCell = ({ row, changeHandler }) => {
    const [readOnly, setReadOnly] = React.useState(row[5]);

    const switchMode = () => {
        row[5] = !row[5];
        setReadOnly(!readOnly);
    };

    const checkEnter = (e) => {
        if (e.key === "Enter" && e.target.value) {
            const old = row[1];
            row[1] = e.target.value;
            if (changeHandler) {
                changeHandler(row, old);
            }
            switchMode();
        } else if (e.key === "Escape") {
            switchMode();
        }
    };

    const revertChange = () => {
        switchMode();
    };

    React.useEffect(() => {
        if (row[5] !== readOnly) {
            setReadOnly(row[5]);
        }
    }, [row[5]]);
    return (
        <div data-tag="allowRowEvents">
            {readOnly && row[1]}
            {!readOnly && <input
                type="text"
                placeholder={row[1]}
                autoFocus
                className="form-control"
                onBlur={revertChange}
                onKeyUp={checkEnter} />}
        </div>
    );
};

// widget for session properties
export function SessionProps({ show, changeHandler }) {
    const sesstionProps = React.useRef({ data: [] });
    const clickedRow = React.useRef([]);
    const [filter, setFilter] = React.useState({ text: '', data: [] });
    const filterData = (event) => {
        const filteredData = sesstionProps.current.data.filter(
            item => item[0].toLowerCase().includes(event.target.value.toLowerCase())
        );
        setFilter({ text: event.target.value, data: filteredData });
    };
    const clearFilter = () => {
        if (filter.text) {
            setFilter({ text: '', data: sesstionProps.current.data });
        }
    };

    const internalChangeHandler = async (row, old) => {
        // update the list to new value first, but verify the new value on the background
        setFilter({ ...filter });
        try {
            await createClient().query(
                `set session ${row[0]}=${row[3] === 'varchar' ? ("'" + row[1] + "'") : row[1]}`
            );
            if (changeHandler) {
                changeHandler(row);
            }
        } catch (e) {
            // revert the session back to previous value and update the list
            row[1] = old;
            setFilter({ ...filter });
        }
    };

    const highlightAltered = [
        {
            when: row => row[1] !== row[2],
            style: {
                fontWeight: 'bold',
                color: '#DFC7F2',
            },
        }
    ];

    const COLUMNS = [
        {
            name: 'Name',
            selector: row => row[0],
            sortable: true,
            wrap: true,
            minwidth: '350px',
        },
        {
            name: 'Current Value',
            selector: row => row[1],
            cell: row => <EditableCell row={row} changeHandler={internalChangeHandler} />,
            width: '150px',
            wrap: true,
        },
        {
            name: 'Default Value',
            selector: row => row[2],
            width: '150px',
            wrap: true,
        },
        {
            name: 'Date Type',
            selector: row => row[3],
            width: '100px',
            wrap: true,
        },
        {
            name: 'Description',
            selector: row => row[4],
            minwidth: '100px',
            wrap: true,
        },
    ];

    const rowClicked = (row) => {
        if (clickedRow.current.length > 0 && clickedRow.current[0] !== row[0]) {
            // change the previous one back to readOnly mode
            clickedRow.current[5] = true;
        }
        // update readOnly as false to trigger the useEffect function of EditableCell
        row[5] = false;
        clickedRow.current = row;
        setFilter({ ...filter });
    };

    React.useEffect(() => {
        //fetch session properties:
        async function getSessionProps() {
            const client = createClient();
            const prestoQuery = await client.query('show session');
            // Add an extra field to store the editing mode (readOnly=true|false)
            sesstionProps.current = { data: prestoQuery.data.map(prop => [...prop, true]) };
            setFilter({ text: '', data: sesstionProps.current.data });
        }
        getSessionProps();
    }, []);

    return (
        <div className={clsx(!show && 'visually-hidden')}>
            <div className="row">
                <div className='col-12'>
                    <DataTable
                        columns={COLUMNS}
                        data={filter.data}
                        theme='dark'
                        customStyles={CUSTOM_STYLES}
                        subHeader
                        onRowClicked={rowClicked}
                        subHeaderComponent={<NameFilter filterText={filter.text} onFilter={filterData} onClear={clearFilter} />}
                        pagination
                        dense
                        conditionalRowStyles={highlightAltered}
                        highlightOnHover
                        persistTableHead />
                </div>
            </div>
        </div>
    );
}
