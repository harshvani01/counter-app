import { useEffect, useState } from "react";

// Relative base path. In production Traefik routes /api to the backend service;
// during local dev Vite proxies /api (see vite.config.js).
const API = "/api/counter";
const STREAM = "/api/counter/stream";

export default function App() {
  // `value` is the authoritative number pushed to us over the SSE stream.
  const [value, setValue] = useState(null);
  // `pending` is an optimistic overlay: writes we've fired but that the server
  // hasn't confirmed back over the stream yet. Makes clicks feel instant.
  const [pending, setPending] = useState(0);
  const [error, setError] = useState(null);

  // COMMAND side: fire an async write. The backend returns 202 Accepted and drops
  // an event on Kafka; a separate consumer applies it to Redis a moment later.
  // We optimistically shift `pending` so the number updates immediately; the SSE
  // stream then delivers the real value. On failure we roll the overlay back.
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

  // Open one Server-Sent Events connection. The server pushes the current value
  // on connect and every time it changes — so we never poll. EventSource also
  // auto-reconnects if the connection drops, re-syncing the value on reconnect.
  useEffect(() => {
    const es = new EventSource(STREAM);
    es.onmessage = (e) => {
      setValue(parseInt(e.data, 10));
      setPending(0); // server confirmed: our optimistic writes have landed
      setError(null);
    };
    es.onerror = () => setError("stream disconnected, reconnecting…");
    return () => es.close();
  }, []);

  const display = value === null ? "—" : value + pending;
  const syncing = pending !== 0;

  return (
    <div className="card">
      <h1>Counter-app</h1>
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
