import { PageTitle } from "./components/PageTitle";
import { createRoot } from 'react-dom/client';
import lazy from "./lazy";

const Splits = lazy('Splits');

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={["Timeline"]} />);

const timeline = createRoot(document.getElementById('timeline'));
timeline.render(<Splits />);
