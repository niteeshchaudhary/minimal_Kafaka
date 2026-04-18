import React, { useState, useEffect } from 'react';
import { ShieldCheck, Lock, UserPlus, Trash2, ShieldAlert, Key } from 'lucide-react';

interface ACLInfo {
  topic: string;
  user: string;
  permission: string;
}

interface TopicInfo {
  name: string;
}

interface SecurityProps {
  topics: TopicInfo[];
  currentUser: string;
}

const Security: React.FC<SecurityProps> = ({ topics, currentUser }) => {
  const [acls, setAcls] = useState<ACLInfo[]>([]);
  const [newAcl, setNewAcl] = useState({ topic: '', user: '', permission: 'Read' });
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState('');
  const [showTLSModal, setShowTLSModal] = useState(false);
  const [tlsConfig, setTlsConfig] = useState({ cert: '', key: '' });

  const fetchAcls = async () => {
    try {
      const res = await fetch('/api/acls', {
        headers: { 'X-User': currentUser }
      });
      if (res.ok) {
        setAcls(await res.json());
      }
    } catch (err) {
      console.error("Failed to fetch ACLs:", err);
    }
  };

  useEffect(() => {
    fetchAcls();
  }, []);

  const handleAddAcl = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newAcl.topic || !newAcl.user) return;
    setLoading(true);
    try {
      const res = await fetch('/api/acls', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'X-User': currentUser
        },
        body: JSON.stringify(newAcl)
      });
      if (res.ok) {
        setNewAcl({ topic: '', user: '', permission: 'Read' });
        fetchAcls();
        setStatus('ACL added successfully');
        setTimeout(() => setStatus(''), 3000);
      }
    } catch (err) {
      console.error("Failed to add ACL:", err);
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteAcl = async (topic: string, user: string) => {
    if (!confirm(`Remove access for ${user} on topic ${topic}?`)) return;
    try {
      const res = await fetch(`/api/acls?topic=${topic}&user=${user}`, { 
        method: 'DELETE',
        headers: { 'X-User': currentUser }
      });
      if (res.ok) {
        fetchAcls();
      }
    } catch (err) {
      console.error("Failed to delete ACL:", err);
    }
  };

  const handleTLSUpdate = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!tlsConfig.cert || !tlsConfig.key) return;
    setLoading(true);
    setStatus('Updating TLS...');
    try {
      const res = await fetch('/api/security/tls', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'X-User': currentUser
        },
        body: JSON.stringify(tlsConfig)
      });
      if (res.ok) {
        setShowTLSModal(false);
        setStatus('TLS configuration updated. Restart broker to apply.');
        setTimeout(() => setStatus(''), 5000);
      } else {
        setStatus(`Error: ${await res.text()}`);
      }
    } catch (err) {
      setStatus(`Failed to update TLS: ${err}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <section>
      <header style={{ marginBottom: '40px' }}>
        <h1 style={{ fontSize: '32px', fontWeight: 600 }}>Security & Governance</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Manage access control lists and security protocols</p>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 350px', gap: '24px' }}>
        <div className="glass" style={{ padding: '24px' }}>
          <h2 style={{ fontSize: '18px', marginBottom: '24px', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Lock size={18} /> Access Control Lists (ACLs)
          </h2>
          
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
              <thead>
                <tr style={{ color: 'var(--text-secondary)', textAlign: 'left' }}>
                  <th style={{ padding: '12px', borderBottom: '1px solid var(--border-color)' }}>TOPIC</th>
                  <th style={{ padding: '12px', borderBottom: '1px solid var(--border-color)' }}>PRINCIPAL</th>
                  <th style={{ padding: '12px', borderBottom: '1px solid var(--border-color)' }}>PERMISSION</th>
                  <th style={{ padding: '12px', borderBottom: '1px solid var(--border-color)', textAlign: 'right' }}>ACTION</th>
                </tr>
              </thead>
              <tbody>
                {acls.map((acl, i) => (
                  <tr key={i}>
                    <td style={{ padding: '12px', borderBottom: '1px solid var(--border-color)', fontWeight: 600 }}>{acl.topic}</td>
                    <td style={{ padding: '12px', borderBottom: '1px solid var(--border-color)' }}>User:{acl.user}</td>
                    <td style={{ padding: '12px', borderBottom: '1px solid var(--border-color)' }}>
                      <span style={{ 
                        padding: '4px 8px', borderRadius: '4px', fontSize: '11px', fontWeight: 600,
                        background: acl.permission === 'Admin' ? 'var(--accent-color)' : 'rgba(255,255,255,0.05)',
                        color: acl.permission === 'Admin' ? 'white' : 'var(--text-primary)'
                      }}>
                        {acl.permission.toUpperCase()}
                      </span>
                    </td>
                    <td style={{ padding: '12px', borderBottom: '1px solid var(--border-color)', textAlign: 'right' }}>
                      <button onClick={() => handleDeleteAcl(acl.topic, acl.user)} style={{ color: 'rgba(255,255,255,0.3)' }} className="hover-danger">
                        <Trash2 size={16} />
                      </button>
                    </td>
                  </tr>
                ))}
                {acls.length === 0 && (
                  <tr>
                    <td colSpan={4} style={{ padding: '40px', textAlign: 'center', color: 'var(--text-secondary)' }}>
                      No ACLs defined. Default policy is "Allow All" for MVP.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
           <div className="glass" style={{ padding: '24px' }}>
              <h3 style={{ fontSize: '16px', marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                <UserPlus size={16} /> Add New ACL
              </h3>
              <form onSubmit={handleAddAcl} style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                <div>
                  <label style={{ display: 'block', fontSize: '11px', color: 'var(--text-secondary)', marginBottom: '4px' }}>TOPIC</label>
                  <select 
                    value={newAcl.topic} 
                    onChange={e => setNewAcl({...newAcl, topic: e.target.value})}
                    style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', color: 'white', padding: '10px', borderRadius: '8px' }}
                  >
                    <option value="">Select Topic...</option>
                    {topics.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
                  </select>
                </div>
                <div>
                  <label style={{ display: 'block', fontSize: '11px', color: 'var(--text-secondary)', marginBottom: '4px' }}>USER ID</label>
                  <input 
                    type="text" 
                    value={newAcl.user}
                    onChange={e => setNewAcl({...newAcl, user: e.target.value})}
                    placeholder="e.g. alice"
                    style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', color: 'white', padding: '10px', borderRadius: '8px' }}
                  />
                </div>
                <div>
                  <label style={{ display: 'block', fontSize: '11px', color: 'var(--text-secondary)', marginBottom: '4px' }}>PERMISSION</label>
                  <select 
                    value={newAcl.permission}
                    onChange={e => setNewAcl({...newAcl, permission: e.target.value})}
                    style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', color: 'white', padding: '10px', borderRadius: '8px' }}
                  >
                    <option value="Read">Read</option>
                    <option value="Write">Write</option>
                    <option value="Admin">Admin</option>
                  </select>
                </div>
                <button 
                  disabled={loading || !newAcl.topic || !newAcl.user}
                  style={{ 
                    background: 'var(--accent-color)', color: 'white', padding: '10px', borderRadius: '8px', 
                    fontWeight: 600, marginTop: '8px', opacity: (loading || !newAcl.topic || !newAcl.user) ? 0.5 : 1
                  }}
                >
                  {loading ? 'Adding...' : 'Add Rule'}
                </button>
                {status && <div style={{ fontSize: '12px', color: 'var(--success)', textAlign: 'center' }}>{status}</div>}
              </form>
           </div>

           <div className="glass" style={{ padding: '24px' }}>
              <h3 style={{ fontSize: '16px', marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                <ShieldAlert size={16} /> Security Status
              </h3>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px' }}>
                   <span style={{ color: 'var(--text-secondary)' }}>TLS Encryption</span>
                   <span style={{ color: 'var(--danger)', fontWeight: 600 }}>DISABLED</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px' }}>
                   <span style={{ color: 'var(--text-secondary)' }}>SASL Auth</span>
                   <span style={{ color: 'var(--success)', fontWeight: 600 }}>ENABLED (Simplified)</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px' }}>
                   <span style={{ color: 'var(--text-secondary)' }}>Principal Mode</span>
                   <span style={{ fontWeight: 600 }}>X-User Header</span>
                </div>
                <button 
                  onClick={() => setShowTLSModal(true)}
                  style={{ marginTop: '12px', fontSize: '12px', color: 'var(--accent-color)', display: 'flex', alignItems: 'center', gap: '4px', background: 'transparent', border: 'none', cursor: 'pointer' }}
                >
                  <Key size={14} /> Configure TLS Certificates
                </button>
              </div>
           </div>
        </div>

        {showTLSModal && (
          <div className="modal-overlay">
            <div className="modal-content glass" style={{ width: '600px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '24px' }}>
                <Key size={24} color="var(--accent-color)" />
                <h3 style={{ fontSize: '20px' }}>Configure TLS Certificates</h3>
              </div>
              <form onSubmit={handleTLSUpdate} style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
                <div>
                  <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>SERVER CERTIFICATE (PEM)</label>
                  <textarea 
                    value={tlsConfig.cert}
                    onChange={e => setTlsConfig({...tlsConfig, cert: e.target.value})}
                    placeholder="-----BEGIN CERTIFICATE-----..."
                    style={{ width: '100%', height: '120px', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', color: 'white', padding: '12px', borderRadius: '8px', fontFamily: 'monospace', fontSize: '12px' }}
                  />
                </div>
                <div>
                  <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>PRIVATE KEY (PEM)</label>
                  <textarea 
                    value={tlsConfig.key}
                    onChange={e => setTlsConfig({...tlsConfig, key: e.target.value})}
                    placeholder="-----BEGIN PRIVATE KEY-----..."
                    style={{ width: '100%', height: '120px', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', color: 'white', padding: '12px', borderRadius: '8px', fontFamily: 'monospace', fontSize: '12px' }}
                  />
                </div>
                <div style={{ padding: '12px', background: 'rgba(239, 68, 68, 0.1)', borderRadius: '8px', fontSize: '12px', color: 'var(--danger)' }}>
                  ⚠️ Applying new certificates will update the broker's configuration but requires a restart to enable encryption on the network layer.
                </div>
                <div style={{ display: 'flex', gap: '12px', justifyContent: 'flex-end', marginTop: '12px' }}>
                  <button type="button" onClick={() => setShowTLSModal(false)} style={{ padding: '10px 20px', borderRadius: '8px', color: 'var(--text-secondary)', fontWeight: 600 }}>Cancel</button>
                  <button type="submit" disabled={loading} style={{ background: 'var(--accent-color)', color: 'white', padding: '10px 20px', borderRadius: '8px', fontWeight: 600, opacity: loading ? 0.5 : 1 }}>
                    {loading ? 'Saving...' : 'Apply Configuration'}
                  </button>
                </div>
              </form>
            </div>
          </div>
        )}
      </div>

      <style>{`
        .hover-danger:hover {
          color: var(--danger) !important;
        }
      `}</style>
    </section>
  );
};

export default Security;
