import { useEffect, useState } from "react";

// Relative base path. In production Traefik routes /api to the backend service;
// during local dev Vite proxies /api (see vite.config.js).
const API = "/api/counter";

export default function App() {
  const [value, setValue] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  // QUERY side: read the current counter value from Redis (via the backend).
  async function refresh() {
    setError(null);
    try {
      const res = await fetch(API);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setValue(data.value);
    } catch (err) {
      setError(err.message);
    }
  }

  // COMMAND side: fire a write. The backend returns 202 Accepted and pushes an
  // event onto Kafka; a separate consumer applies it to Redis a moment later.
  // So we don't get a value back — we re-fetch shortly after (eventual
  // consistency). Under heavy load the value may briefly lag behind.
  async function command(path) {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API}${path}`, { method: "POST" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setTimeout(refresh, 250); // give the consumer a moment, then re-query
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
  }, []);

  return (
    <div className="card">
      <h1>Counter</h1>
      <div className="value">{value === null ? "—" : value}</div>
      <div className="buttons">
        <button onClick={() => command("/decrement")} disabled={loading}>
          −
        </button>
        <button onClick={() => command("/increment")} disabled={loading}>
          +
        </button>
      </div>
      <button
        className="reset"
        onClick={() => command("/reset")}
        disabled={loading}
      >
        Reset
      </button>
      <button className="reset" onClick={refresh} disabled={loading}>
        Refresh
      </button>
      {error && <p className="error">Error: {error}</p>}
    </div>
  );
}
