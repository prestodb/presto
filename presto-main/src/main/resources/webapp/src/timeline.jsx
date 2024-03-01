import Splits from "./components/Splits";
import { PageTitle } from "./components/PageTitle";
import { createRoot } from 'react-dom/client';

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={["Timeline"]} />);

const timeline = createRoot(document.getElementById('timeline'));
timeline.render(<Splits />);
