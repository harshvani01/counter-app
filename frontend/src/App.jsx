import { useEffect, useState } from "react";

// Relative base path. In production Traefik routes /api to the backend service;
// during local dev Vite proxies /api (see vite.config.js).
const API = "/api/counter";

export default function App() {
  const [value, setValue] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function call(path, method = "GET") {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API}${path}`, { method });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setValue(data.value);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    call("");
  }, []);

  return (
    <div className="card">
      <h1>Counter</h1>
      <div className="value">{value === null ? "—" : value}</div>
      <div className="buttons">
        <button onClick={() => call("/decrement", "POST")} disabled={loading}>
          −
        </button>
        <button onClick={() => call("/increment", "POST")} disabled={loading}>
          +
        </button>
      </div>
      <button
        className="reset"
        onClick={() => call("/reset", "POST")}
        disabled={loading}
      >
        Reset
      </button>
      {error && <p className="error">Error: {error}</p>}
    </div>
  );
}
