import { useEffect, useState } from "react";

// Relative base path. In production Traefik routes /api to the backend service;
// during local dev Vite proxies /api (see vite.config.js).
const API = "/api/counter";
const POLL_MS = 1000;

export default function App() {
  // `value` is the authoritative number last read from Redis (via the backend).
  const [value, setValue] = useState(null);
  // `pending` is an optimistic overlay: the net effect of writes we've fired but
  // that the Kafka consumer may not have applied to Redis yet. It makes the UI
  // feel instant even though the backend is eventually consistent.
  const [pending, setPending] = useState(0);
  const [error, setError] = useState(null);

  // QUERY side: read the current value. Never blanks the display on a transient
  // error — we keep showing the last known number.
  async function refresh() {
    try {
      const res = await fetch(API);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setValue(data.value);
      setPending(0); // the server value moved: our optimistic writes have landed
      setError(null);
    } catch (err) {
      setError(err.message);
    }
  }

  // COMMAND side: fire an async write. The backend returns 202 Accepted and drops
  // an event on Kafka; a separate consumer applies it to Redis a moment later.
  // We optimistically shift `pending` so the number updates immediately, then the
  // next poll reconciles with the real value. On failure we roll the overlay back.
  async function command(path, delta) {
    setPending((p) => p + delta);
    try {
      const res = await fetch(`${API}${path}`, { method: "POST" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setError(null);
    } catch (err) {
      setPending((p) => p - delta);
      setError(err.message);
    }
  }

  // Poll the authoritative value once per second so the UI self-heals without
  // any manual refresh — this is how a client copes with an async backend.
  useEffect(() => {
    refresh();
    const id = setInterval(refresh, POLL_MS);
    return () => clearInterval(id);
  }, []);

  const display = value === null ? "—" : value + pending;
  const syncing = pending !== 0;

  return (
    <div className="card">
      <h1>Counter</h1>
      <div className="value">{display}</div>
      <div className="status">{syncing ? "syncing…" : "\u00a0"}</div>
      <div className="buttons">
        <button onClick={() => command("/decrement", -1)}>−</button>
        <button onClick={() => command("/increment", +1)}>+</button>
      </div>
      <button className="reset" onClick={() => command("/reset", 0)}>
        Reset
      </button>
      {error && <p className="error">Error: {error}</p>}
    </div>
  );
}
