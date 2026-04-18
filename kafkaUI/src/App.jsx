import React, { useState, useEffect, useCallback } from 'react';
import { Database, UserCheck, Shield, ChevronRight, Activity, Plus, Trash2, RefreshCw } from 'lucide-react';
import './App.css';

const API = 'http://localhost:8080';

export default function App() {
  const [tab, setTab] = useState('topics');
  const [activeTopic, setActiveTopic] = useState(null);

  const view = () => {
    if (activeTopic) return <MessagesView topic={activeTopic} onBack={() => setActiveTopic(null)} />;
    switch (tab) {
      case 'topics': return <TopicsView onSelect={setActiveTopic} />;
      case 'groups': return <ConsumerGroupsView />;
      case 'acls': return <ACLView />;
      default: return <TopicsView onSelect={setActiveTopic} />;
    }
  };

  return (
    <div className="dashboard-container">
      <aside className="sidebar">
        <h1><Activity color="#3b82f6" /> Kafka Admin</h1>
        <nav className="nav-menu">
          {[['topics', Database, 'Topics'], ['groups', UserCheck, 'Consumer Groups'], ['acls', Shield, 'ACLs']].map(([id, Icon, label]) => (
            <div key={id} className={`nav-item ${tab === id && !activeTopic ? 'active' : ''}`} onClick={() => { setTab(id); setActiveTopic(null); }}>
              <Icon size={20} /> {label}
            </div>
          ))}
        </nav>
      </aside>
      <main className="main-content">{view()}</main>
    </div>
  );
}

/* ─── Topics ────────────────────────────────────────────────────────── */
function TopicsView({ onSelect }) {
  const [topics, setTopics] = useState([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(() => {
    setLoading(true);
    fetch(`${API}/topics`).then(r => r.json()).then(setTopics).catch(console.error).finally(() => setLoading(false));
  }, []);

  useEffect(load, [load]);

  return (
    <div>
      <div className="page-header">
        <h2>Topics</h2>
        <p>Real-time state of topics in your broker.</p>
      </div>
      <div className="glass-panel">
        <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 16 }}>
          <button className="primary" onClick={load} style={{ display: 'flex', alignItems: 'center', gap: 6 }}><RefreshCw size={14} /> Refresh</button>
        </div>
        {loading ? <div className="loader"></div> : (
          <table className="data-table">
            <thead><tr><th>Name</th><th>Partitions</th><th>Total Messages</th><th>Actions</th></tr></thead>
            <tbody>
              {topics.length === 0
                ? <tr><td colSpan="4" style={{ color: 'var(--text-secondary)' }}>No topics found. Produce a message to create one.</td></tr>
                : topics.map(t => (
                  <tr key={t.name}>
                    <td style={{ fontWeight: 500 }}>{t.name}</td>
                    <td>{t.partitions}</td>
                    <td>{t.messages_count}</td>
                    <td><span className="table-action" onClick={() => onSelect(t.name)}>View Messages</span></td>
                  </tr>
                ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

/* ─── Messages ──────────────────────────────────────────────────────── */
function MessagesView({ topic, onBack }) {
  const [msgs, setMsgs] = useState([]);
  const [part, setPart] = useState(0);
  const [off, setOff] = useState(0);
  const [loading, setLoading] = useState(false);

  const scan = () => {
    setLoading(true);
    fetch(`${API}/consume?topic=${topic}&partition=${part}&offset=${off}`)
      .then(r => r.json()).then(d => setMsgs(d || [])).catch(console.error).finally(() => setLoading(false));
  };

  useEffect(scan, [topic]);

  return (
    <div>
      <div className="page-header" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <span className="table-action" onClick={onBack}>Topics</span>
        <ChevronRight size={16} color="#94a3b8" />
        <h2 style={{ margin: 0 }}>{topic} / Messages</h2>
      </div>
      <div className="glass-panel">
        <div className="form-row">
          <div className="input-group"><label>Partition</label><input type="number" min="0" value={part} onChange={e => setPart(e.target.value)} /></div>
          <div className="input-group"><label>Start Offset</label><input type="number" min="0" value={off} onChange={e => setOff(e.target.value)} /></div>
          <button className="primary" onClick={scan}>Scan</button>
        </div>
        {loading ? <div className="loader"></div> : (
          <div>{msgs.length === 0 ? <p style={{ color: 'var(--text-secondary)' }}>No messages found.</p> : msgs.map((m, i) => (
            <div key={i} className="message-item">
              <div className="message-meta">
                <span><strong>Offset:</strong> {m.offset}</span>
                <span><strong>Key:</strong> {m.key || 'null'}</span>
                <span><strong>Timestamp:</strong> {new Date(m.timestamp).toLocaleString()}</span>
              </div>
              <div className="message-value">{m.value}</div>
            </div>
          ))}</div>
        )}
      </div>
    </div>
  );
}

/* ─── Consumer Groups ───────────────────────────────────────────────── */
function ConsumerGroupsView() {
  const [groups, setGroups] = useState([]);
  const [loading, setLoading] = useState(true);
  const [newId, setNewId] = useState('');

  const load = useCallback(() => {
    setLoading(true);
    fetch(`${API}/consumer-groups`).then(r => r.json()).then(setGroups).catch(console.error).finally(() => setLoading(false));
  }, []);

  useEffect(load, [load]);

  const create = () => {
    if (!newId.trim()) return;
    fetch(`${API}/consumer-groups`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ groupId: newId.trim() }) })
      .then(() => { setNewId(''); load(); });
  };

  const remove = (gid) => {
    fetch(`${API}/consumer-groups?groupId=${encodeURIComponent(gid)}`, { method: 'DELETE' }).then(load);
  };

  return (
    <div>
      <div className="page-header">
        <h2>Consumer Groups</h2>
        <p>Monitor real consumer group state and lag.</p>
      </div>

      <div className="glass-panel">
        <div className="form-row">
          <div className="input-group"><label>New Group ID</label><input value={newId} onChange={e => setNewId(e.target.value)} placeholder="e.g. my-consumer-group" /></div>
          <button className="primary" onClick={create} style={{ display: 'flex', alignItems: 'center', gap: 6 }}><Plus size={14} /> Create</button>
          <button className="primary" onClick={load} style={{ display: 'flex', alignItems: 'center', gap: 6, background: 'var(--border-color)' }}><RefreshCw size={14} /> Refresh</button>
        </div>
      </div>

      <div className="glass-panel">
        {loading ? <div className="loader"></div> : (
          <table className="data-table">
            <thead><tr><th>Group ID</th><th>State</th><th>Members</th><th>Lag</th><th>Actions</th></tr></thead>
            <tbody>
              {groups.length === 0
                ? <tr><td colSpan="5" style={{ color: 'var(--text-secondary)' }}>No consumer groups. Create one above.</td></tr>
                : groups.map(g => (
                  <tr key={g.groupId}>
                    <td><strong>{g.groupId}</strong></td>
                    <td><span className={`badge ${g.state === 'Stable' ? 'success' : 'warning'}`}>{g.state}</span></td>
                    <td>{g.members}</td>
                    <td>{g.lag}</td>
                    <td><span className="table-action" style={{ color: '#ef4444' }} onClick={() => remove(g.groupId)}><Trash2 size={14} /></span></td>
                  </tr>
                ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

/* ─── ACLs ──────────────────────────────────────────────────────────── */
function ACLView() {
  const [acls, setAcls] = useState([]);
  const [loading, setLoading] = useState(true);
  const [form, setForm] = useState({ principal: '', resourceType: 'Topic', resourceName: '', operation: 'Read' });

  const load = useCallback(() => {
    setLoading(true);
    fetch(`${API}/acls`).then(r => r.json()).then(setAcls).catch(console.error).finally(() => setLoading(false));
  }, []);

  useEffect(load, [load]);

  const add = () => {
    if (!form.principal || !form.resourceName) return;
    fetch(`${API}/acls`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(form) })
      .then(() => { setForm({ principal: '', resourceType: 'Topic', resourceName: '', operation: 'Read' }); load(); });
  };

  const remove = (idx) => {
    fetch(`${API}/acls?index=${idx}`, { method: 'DELETE' }).then(load);
  };

  return (
    <div>
      <div className="page-header">
        <h2>ACLs</h2>
        <p>Manage access control entries for your cluster.</p>
      </div>

      <div className="glass-panel">
        <div className="form-row">
          <div className="input-group"><label>Principal</label><input value={form.principal} onChange={e => setForm({ ...form, principal: e.target.value })} placeholder="User:admin" /></div>
          <div className="input-group">
            <label>Resource Type</label>
            <select value={form.resourceType} onChange={e => setForm({ ...form, resourceType: e.target.value })}>
              <option>Topic</option><option>Cluster</option><option>Group</option><option>TransactionalId</option>
            </select>
          </div>
          <div className="input-group"><label>Resource Name</label><input value={form.resourceName} onChange={e => setForm({ ...form, resourceName: e.target.value })} placeholder="events" /></div>
          <div className="input-group">
            <label>Operation</label>
            <select value={form.operation} onChange={e => setForm({ ...form, operation: e.target.value })}>
              <option>Read</option><option>Write</option><option>All</option><option>Describe</option><option>Create</option><option>Delete</option>
            </select>
          </div>
          <button className="primary" onClick={add} style={{ display: 'flex', alignItems: 'center', gap: 6 }}><Plus size={14} /> Add</button>
        </div>
      </div>

      <div className="glass-panel">
        {loading ? <div className="loader"></div> : (
          <table className="data-table">
            <thead><tr><th>Principal</th><th>Resource Type</th><th>Resource Name</th><th>Operation</th><th>Actions</th></tr></thead>
            <tbody>
              {acls.length === 0
                ? <tr><td colSpan="5" style={{ color: 'var(--text-secondary)' }}>No ACLs defined. Add one above.</td></tr>
                : acls.map((a, i) => (
                  <tr key={i}>
                    <td>{a.principal}</td>
                    <td>{a.resourceType}</td>
                    <td>{a.resourceName}</td>
                    <td>{a.operation}</td>
                    <td><span className="table-action" style={{ color: '#ef4444' }} onClick={() => remove(i)}><Trash2 size={14} /></span></td>
                  </tr>
                ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
