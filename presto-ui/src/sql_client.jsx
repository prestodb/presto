import { createRoot } from 'react-dom/client';
import { PageTitle } from "./components/PageTitle";
import 'prismjs/themes/prism-okaidia.css';
import lazy from "./lazy";

const SQLClientView = lazy('SQLClient');

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={['Cluster Overview', 'Resource Groups', 'SQL Client']} urls={['./index.html', './res_groups.html', './sql_client.html']} current={2} />);

const resourceGroups = createRoot(document.getElementById('sql-client'));
resourceGroups.render(<SQLClientView />);
