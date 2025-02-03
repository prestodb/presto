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
import { clsx } from 'clsx';
import { createTheme } from 'react-data-table-component';
import { PrestoQuery } from '@prestodb/presto-js-client'
import { QueryResults } from './QueryResults.jsx';
import { SessionProps } from './SessionProps.jsx';
import { SQLInput, createClient } from './SQLInput.jsx';

createTheme('dark', {
    background: {
      default: 'transparent',
    },
});

type SessionValues = {
    [key: string]: string;
};

export default function SQLClientView() {
    const [values, setValues] = React.useState({sql: '', running: false, results: undefined, view: 'SQL'});
    const sessions: SessionValues = React.useRef({});
    const views = [{name: 'SQL', label: 'SQL'}, {name: 'Session', label: 'Session Properties'}];

    const executeSQL = (sqlInfo) => {
        const client = createClient(
            sqlInfo.catalog,
            sqlInfo.schema,
            Object.keys(sessions.current).map(key => `${key}=${sessions.current[key]}`).join(', ')
        );
        setValues({ ...values, running: true, results: undefined });
        client.query(sqlInfo.sql).then((prestoQuery: PrestoQuery) => {
            setValues({ ...values, running: false, results: prestoQuery });
        })
        .catch((e) => {
            setValues({...values, results: {error: e}});
        });
    };

    const switchView = (view) => {
        if (view.name === values.view) return;
        setValues({...values, view: view.name});
    };

    const handleError = (err) => {
        setValues({...values, results: {error: {message: err.message}}});
    };

    const sessionHandler = ([name, value, defaultValue]) => {
        if (sessions.current[name] && value === defaultValue) {
            // revert the change back to default
            delete sessions.current[name];
        } else {
            sessions.current[name] = value;
        }
    }

    return (
        <>
            <div className="alert alert-warning alert-dismissible fade show" role="alert">
                SQL client directly accesses the coordinator APIs and submits SQL queries. Users who can access the Web UI can use this client to query,
                update, and even delete data in the catalogs. Be sure to enable the user authentication to protect the Web UI access if needed.
                By default, the SQL client uses the <strong>prestoui</strong> user id. You can set
                up <a className="link-offset-2 link-offset-3-hover link-underline link-underline-opacity-0 link-underline-opacity-75-hover" href='http://prestodb.io/docs/current/security/built-in-system-access-control.html' target='_blank'>system access controls</a>
                &nbsp;or <a className="link-offset-2 link-offset-3-hover link-underline link-underline-opacity-0 link-underline-opacity-75-hover" href='http://prestodb.io/docs/current/security/authorization.html' target='_blank'>authorization policies</a> to
                restrict access from the SQL client. Check detailed <a className="link-offset-2 link-offset-3-hover link-underline link-underline-opacity-0 link-underline-opacity-75-hover"href='http://prestodb.io/docs/current/security.html' target="_blank">documentation</a>.
                <button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>
            <div className='container'>
                <div className="row">
                    <div className="col-12">
                        <nav className="nav nav-tabs">
                            {views.map((view, idx) => (
                                <a className={clsx('nav-link', values.view === view.name && 'active')} href="#" onClick={() => switchView(view)}>{view.label}</a>
                            ))}
                        </nav>
                    </div>
                    <div className="col-12">
                        <hr className="h3-hr"/>
                    </div>
                    <div className="col-12">
                        <SQLInput
                            handleSQL={executeSQL}
                            show={values.view === 'SQL'}
                            enabled={!values.running}
                            initialSQL={values.sql}
                            errorHandler={handleError}
                        />
                        <SessionProps show={values.view === 'Session'} changeHandler={sessionHandler} />
                    </div>
                    <div className="col-12">
                        {values.running && <div className="loader">Loading...</div>}
                    </div>
                    <div className="col-12">
                        {values.results && <QueryResults results={values.results} />}
                    </div>
                </div>
            </div>
        </>
    );

}

