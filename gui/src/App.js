import {BrowserRouter, Routes, Route, Outlet} from "react-router-dom";
import {Navbar, Button, Alignment} from "@blueprintjs/core";
import './App.css';

import Status from "./pages/Status";

function AppNavbar() {
  return (
    <Navbar className="bp4-dark">
        <Navbar.Group align={Alignment.LEFT}>
            <Navbar.Heading>Microflow</Navbar.Heading>
            <Navbar.Divider />
            <Button className="bp4-minimal" icon="pulse" text="Status" />
            <Button className="bp4-minimal" icon="walk" text="Runs" />
            <Button className="bp4-minimal" icon="multi-select" text="Partitions" />
        </Navbar.Group>
    </Navbar>
  );
}

function Layout() {
  return (
    <>
      <AppNavbar />
      <Outlet />
    </>
  );
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Status />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;
