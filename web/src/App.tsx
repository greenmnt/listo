import { Routes, Route, Navigate } from "react-router-dom";
import Layout from "./components/Layout";
import ProjectsPage from "./pages/ProjectsPage";
import ApplicationsPage from "./pages/ApplicationsPage";
import ApplicationDetailPage from "./pages/ApplicationDetailPage";
import MapPage from "./pages/MapPage";
import TrendsPage from "./pages/TrendsPage";
import CalculatorPage from "./pages/CalculatorPage";
import AboutPage from "./pages/AboutPage";

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<ProjectsPage />} />
        <Route path="/applications" element={<ApplicationsPage />} />
        <Route
          path="/applications/:slug/:appId"
          element={<ApplicationDetailPage />}
        />
        <Route path="/map" element={<MapPage />} />
        <Route path="/trends" element={<TrendsPage />} />
        <Route path="/calculator" element={<CalculatorPage />} />
        <Route path="/about" element={<AboutPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Layout>
  );
}
