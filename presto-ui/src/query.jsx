import React from "react";
import {QueryDetail} from "./components/QueryDetail";
import {PageTitle} from "./components/PageTitle";
import { createRoot } from 'react-dom/client';

const container = document.getElementById('title');
const root = createRoot(container);

root.render(
    <PageTitle titles={["Query Details"]} />
);

const query = document.getElementById('query-detail');
const query_root = createRoot(query);

query_root.render(
    <QueryDetail />
);
