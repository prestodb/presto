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
import PrestoClient from '@prestodb/presto-js-client'
import DataTable, {createTheme} from 'react-data-table-component';
import antlr4 from 'antlr4';
import SqlBaseLexer from '../sql-parser/SqlBaseLexer.js';
import SqlBaseParser from '../sql-parser/SqlBaseParser.js';
import SqlBaseListener from '../sql-parser/SqlBaseListener.js';
import Editor from 'react-simple-code-editor';
import { highlight, languages } from 'prismjs/components/prism-core';
import 'prismjs/components/prism-sql';
import 'prismjs/themes/prism-okaidia.css';

createTheme('dark', {
    background: {
      default: 'transparent',
    },
});

// UpperCase CharStream
class UpperCaseCharStream extends antlr4.CharStream {
    constructor(data: string) {
        super(data);
    }
    // Override
    LA(offset: number): number {
        const c = super.LA(offset);
        if (c <= 0) {
            return c;
        }
        if (c >=97 && c <= 122) {
            return c - 32;
        }
        return c
    }
}

// Use this to detect the `SELECT`
class SelectListener extends SqlBaseListener {
    limit = -1;
    fetchFirstNRows = -1;
    isSelect = false;

    constructor() {
        super();
    }

    enterQueryNoWith(ctx) {
        super.enterQueryNoWith(ctx);
        this.isSelect = true;
    }
    exitQueryNoWith(ctx) {
        super.exitQueryNoWith(ctx);
        this.limit = ctx.limit ? ctx.limit.text : -1;
        this.fetchFirstNRows = ctx.fetchFirstNRows ? ctx.fetchFirstNRows.text : -1;
    }
}

class SyntaxError extends antlr4.error.ErrorListener {
    error = undefined;

    syntaxError(recognizer, offendingSymbol, line, column, e, f) {
        this.error = new Error(`line: ${line}, column:${column}, msg: ${e}`);
    }
}

// utility function: create PrestoClient which always uses the current domain
function createClient(catalog, schema) {
    const opt = {
        host: `${window.location.protocol}//${window.location.hostname}`,
        port: window.location.port,
        user: 'prestoui',
    };
    if (catalog) {
        opt.catalog = catalog;
    }
    if (schema) {
        opt.schema = schema;
    }
    return new PrestoClient(opt);
}

// Dropdown for Catalog and Schema
function SQLDropDown({text, values, onSelect}) {
    const items = values || [];
    const [data, setData] = React.useState({items: items, current: undefined});

    function selectItem(item) {
        if (item !== data.current) {
            onSelect(item);
            setData({...data, current: item});
        }
    }

    return (
        <div className="btn-xs btn-group dropdown">
            <button type="button" className="btn btn-default dropdown-toggle"
                data-toggle="dropdown" aria-haspopup="true"
                aria-expanded="false">{text} <span className="caret"></span></button>
            <ul className="dropdown-menu">
                {items.map((item, idx) => (
                    <li key={idx}><a href="#" className={data.current === item? 'selected': ''} onClick={() => selectItem(item)}>{item}</a></li>
                ))}
            </ul>
        </div>
    );
}

function sqlCleaning(sqlInfo, errorHandler) {
    const lastSemicolon = /(\s*;\s*)$/m;
    const limitRE = /limit\s+(\d+|all)$/im;
    const fetchFirstRE = /fetch\s+first\s+(\d+)\s+rows\s+only$/im;
    const syntaxError = new SyntaxError();

    try {
        sqlInfo.sql = sqlInfo.sql.replace(lastSemicolon, '').trim();
        const parser = new SqlBaseParser(new antlr4.CommonTokenStream(new SqlBaseLexer(new UpperCaseCharStream(sqlInfo.sql))));
        const selectDetector = new SelectListener();
        parser.addErrorListener(syntaxError);
        antlr4.tree.ParseTreeWalker.DEFAULT.walk(selectDetector, parser.statement());
        //check syntax error
        if (syntaxError.error) {
            errorHandler(syntaxError.error);
            return false;
        }
        if(selectDetector.isSelect) {
            if (typeof(selectDetector.limit) === 'string' || selectDetector.limit > 100) {
                sqlInfo.sql = sqlInfo.sql.replace(limitRE, 'limit 100');
            } else if (selectDetector.fetchFirstNRows > 100) {
                sqlInfo.sql = sqlInfo.sql.replace(fetchFirstRE, 'fetch first 100 rows only');
            } else if (selectDetector.limit === -1 && selectDetector.fetchFirstNRows === -1) {
                sqlInfo.sql += ' limit 100';
            }
        }
    } catch (err) {
        if (errorHandler) {
            errorHandler(err);
        }
        return false;
    }
    return true;
}

// SQL Input, including sql text, catalog(optional), and schema(optional)
function SQLInput({handleSQL, enabled, initialSQL, errorHandler}) {
    const sqlInfo = React.useRef({sql: undefined, catalog: undefined, schema: undefined});
    const [data, setData] = React.useState({catalogs: [], schemas: []});
    const [code, setCode] = React.useState(initialSQL);
    const isDisabled = enabled ? '' : ' disabled';

    const checkValue = () => {
        if (code.length > 0) {
            sqlInfo.current.sql = code;
            if (sqlCleaning(sqlInfo.current, errorHandler)) {
                setCode(sqlInfo.current.sql);
                handleSQL(sqlInfo.current);
            }
        }
    }

    async function setCatalog(catalog) {
        sqlInfo.current.catalog = catalog;
        // also reset schema
        setData({...data, schemas: []});
        // fetch schemas from the catalog
        const client = createClient();
        const prestoQuery = await client.query(`show schemas from ${catalog}`);
        setData({...data, schemas: prestoQuery.data});
    }

    function setSchema(schema) {
        sqlInfo.current.schema = schema;
        setData({...data});
    }

    React.useEffect(() => {
        //fetch catalogs:
        async function getCatalogs() {
            const client = createClient();
            const prestoQuery = await client.query('show catalogs');
            setData({...data, catalogs: prestoQuery.data});
        }
        getCatalogs();
    }, [initialSQL]);

    return (
        <>
            <div className="row">
                <div className="col-xs-12">
                    <div className='input-group' role='group'>
                        <SQLDropDown text='Catalog' values={data.catalogs.map(i => i[0])} onSelect={setCatalog} />
                        <SQLDropDown text='Schema' values={data.schemas.map(i => i[0])} onSelect={setSchema} />
                        <div className="btn-xs btn-group">
                            <button className={'btn btn-success' + isDisabled} type="button" onClick={checkValue}>Run</button>
                        </div>
                    </div>
                </div>
            </div>
            <div className="row">
                <div className="col-xs-12">
                    <Editor
                        value={code}
                        onValueChange={code => setCode(code)}
                        highlight={code => highlight(code, languages.sql)}
                        padding={10}
                        style={{
                            fontFamily: '"Fira code", "Fira Mono", monospace',
                            fontSize: 14,
                            borderWidth: '2px',
                            borderColor: 'black',
                            border: 'solid',
                            margin: '4px',
                        }}
                        placeholder='Type your SQL query...'
                    />
                </div>
            </div>
        </>
    );
}

function QueryResults({results}) {
    const customStyles = {
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

    const getColumns = () => {
        return results.columns.map( (row, index) => {
            return {
                name: row.name,
                selector: row => row[index],
            };
        });
    };

    return (
        <>
            <div className="row">
                <div className='col-xs-6'>
                    <h3>Results</h3>
                </div>
                <div className="col-xs-6">
                    {results.queryId &&
                        <a style={{display: 'block', marginTop: '27px', marginBottom: '10px'}}
                            href={"query.html?" + results.queryId} target="_blank"
                            data-toggle="tooltip" data-trigger="hover" title="Query ID"
                        >
                            {results.queryId}
                        </a>
                    }
                </div>
            </div>
            <div className="row"><hr className="h3-hr"/></div>
            {results.error && <div className="row"><div className="alert alert-danger" role="alert"><h4 className="text-center">{results.error.message}</h4></div></div>}
            {results.data && <div className="row"><DataTable columns={getColumns()} data={results.data} theme='dark' customStyles={customStyles} striped='true' pagination /></div>}
        </>
    );
}

export default function SQLClientView() {
    const [values, setValues] = React.useState({sql: '', running: false, results: undefined});

    const executeSQL = (sqlInfo) => {
        const client = createClient(sqlInfo.catalog, sqlInfo.schema);
        setValues({ ...values, running: true, results: undefined });
        client.query(sqlInfo.sql).then((prestoQuery) => {
            if (prestoQuery.data && prestoQuery.data.length > 0) {
                setValues({ ...values, running: false, results: prestoQuery });
            } else {
                // presto-js-client doesn't return error
                // need to query the error on our own
                fetch('/v1/query/' + prestoQuery.queryId)
                    .then(response => response.json())
                    .then(query => {
                        if (query.errorCode) {
                            setValues({
                                ...values, running: false, results: {
                                    error: {
                                        code: query.errorCode.name,
                                        message: query.failureInfo.message,
                                    }
                                }
                            });
                        } else {
                            // the query returns zero record
                            setValues({ ...values, running: false, results: prestoQuery });
                        }
                    })
                    .catch((error) => {
                        setValues({ ...values, running: false, results: { error } });
                    });
            }
        })
        .catch((e) => {
            // presto-js-client only return error type in the error message
            // it would be good to also have detailed message
            setValues({...values, results: {error: e}});
        });
    };

    const handleError = (err) => {
        setValues({...values, results: {error: {message: err.message}}});
    }

    return (
        <>
            <div className="alert alert-warning alert-dismissible" role="alert" style={{display: 'block', marginLeft: 'auto', marginRight: 'auto', textAlign: 'center'}}>
                <button type="button" className="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                SQL client directly accesses the coordinator APIs and submits SQL queries. Users who can access the Web UI can use this client to query,
                update, and even delete data in the catalogs. Be sure to enable the user authentication to protect the Web UI access if needed.
                By default, the SQL client uses the <strong>prestoui</strong> user id. You can set
                up <a className="alert-link" href='http://prestodb.io/docs/current/security/built-in-system-access-control.html' target='_blank'>system access controls</a>
                &nbsp;or <a className="alert-link" href='http://prestodb.io/docs/current/security/authorization.html' target='_blank'>authorization policies</a> to
                restrict access from the SQL client. Check detailed <a className="alert-link" href='http://prestodb.io/docs/current/security.html' target="_blank">documentation</a>.
            </div>
            <div className='container'>
                <div className="row">
                    <div className="col-xs-12">
                        <h3>SQL</h3>
                        <hr className="h3-hr"/>
                        <SQLInput handleSQL={executeSQL} enabled={!values.running} initialSQL={values.sql} errorHandler={handleError}/>
                    </div>
                    <div className="col-xs-12">
                        {values.running && <div className="loader">Loading...</div>}
                    </div>
                    <div className="col-xs-12">
                        {values.results && <QueryResults results={values.results} />}
                    </div>
                </div>
            </div>
        </>
    );

}

