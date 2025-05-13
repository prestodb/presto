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

import PlanView from './QueryPlanView';
import QueryOverview from './QueryOverview';
import SplitView from './QuerySplitsView';
import StageView from './QueryStageView';
import StaticQueryHeader from './StaticQueryHeader';

// A form to select a JSON file and read
const FileForm = ({ onChange }) => (
    <div className="row">
        <div className="col-4 offset-1 fs-6 mb-2">
            <div id="title">Select a JSON file of SQL query to process</div>
            <form id='form' className="form-inline">
                <div className="form-group">
                    <input id='file' type="file" name="file" accept='.json, application/json' onChange={onChange}/>
                </div>
            </form>
        </div>
    </div>
);

export function QueryViewer() {
    const [state, setState] = React.useState({
        initialized: false,
        ended: false,
        tab: 'overview',
        query: null,
    });

    const tabs = [
        {name: 'overview', label: 'Overview'},
        {name: 'plan', label: 'Plan'},
        {name: 'stage', label: 'Stage Performance'},
        {name: 'splits', label: 'Splits'},
    ];

    const switchTab = (tab) => {
        setState({...state, tab: tab.name});
    };

    const readJSON = (e) => {
        if (!e.target.files[0]) {
            return;
        }
        const fr = new FileReader();
        fr.onload = function () {
            if (!fr.result) {
                return;
            }
            try {
                const queryJSON = JSON.parse(fr.result);
                setState({
                    ...state,
                    initialized: true,
                    ended: queryJSON.finalQueryInfo,
                    query: queryJSON,
                });
            } catch (err) {
                console.err(err);
            }
        }
        fr.readAsText(e.target.files[0]);
    };


    return (
        <div>
            <FileForm onChange={readJSON} />
            <StaticQueryHeader query={state.query} tabs={tabs} switchTab={switchTab}/>
            <QueryOverview data={state.query} show={state.tab === 'overview'} />
            <PlanView data={state.query} show={state.tab === 'plan'} />
            <StageView data={state.query} show={state.tab === 'stage'} />
            <SplitView data={state.query} show={state.tab === 'splits'} />
        </div>
    );
}

export default QueryViewer;