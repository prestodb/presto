import React from "react";
import {ClusterHUD} from "./ClusterHUD";
import {QueryList} from "./QueryList";
import {PageTitle} from "./PageTitle";

import { createRoot } from 'react-dom/client';
const container = document.getElementById('title');
const root = createRoot(container);

root.render(
    <PageTitle titles={['Cluster Overview']} urls={['./index.html']} current={0}/>
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
