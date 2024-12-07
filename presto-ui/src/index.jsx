import React from "react";
import {ClusterHUD} from "./components/ClusterHUD";
import {QueryList} from "./components/QueryList";
import { PageTitle } from "./components/PageTitle";
import { createRoot } from 'react-dom/client';


const container = document.getElementById('title');
const root = createRoot(container);

root.render(
    <PageTitle titles={['Cluster Overview', 'Resource Groups', 'SQL Client']} urls={['./index.html', 'res_groups.html', 'sql_client.html']} current={0}/>
);

const cluster = document.getElementById('cluster-hud');
const cluster_root = createRoot(cluster);
cluster_root.render(
    <ClusterHUD />
);

const query = document.getElementById('query-list');
const query_root = createRoot(query);

query_root.render(
    <QueryList />
);
