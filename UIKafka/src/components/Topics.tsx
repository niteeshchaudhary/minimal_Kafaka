import React, { useState } from 'react';
import { Database, Box, Plus, Trash2, X, Info, AlertCircle, Settings, Clock, Save } from 'lucide-react';

interface PartitionMetadata {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
}

interface TopicInfo {
  name: string;
  partitions: PartitionMetadata[];
}

interface TopicsProps {
  topics: TopicInfo[];
  onRefresh: () => void;
  currentUser: string;
}

const Topics: React.FC<TopicsProps> = ({ topics, onRefresh, currentUser }) => {
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newTopicName, setNewTopicName] = useState('');
  const [newTopicPartitions, setNewTopicPartitions] = useState(3);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [settingsTopic, setSettingsTopic] = useState<TopicInfo | null>(null);

  const handleCreateTopic = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTopicName) return;
    setLoading(true);
    setError('');
    try {
      const res = await fetch('/api/topics', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'X-User': currentUser
        },
        body: JSON.stringify({ name: newTopicName, partitions: newTopicPartitions })
      });
      if (res.ok) {
        setShowCreateModal(false);
        setNewTopicName('');
        setNewTopicPartitions(3);
        onRefresh();
      } else {
        setError(await res.text());
      }
    } catch (err) {
      setError(`Failed to create topic: ${err}`);
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteTopic = async (name: string) => {
    if (!confirm(`Are you sure you want to delete topic "${name}"? This action cannot be undone.`)) return;
    try {
      const res = await fetch(`/api/topics?name=${name}`, { 
        method: 'DELETE',
        headers: { 'X-User': currentUser }
      });
      if (res.ok) {
        onRefresh();
      } else {
        alert(`Error deleting topic: ${await res.text()}`);
      }
    } catch (err) {
      alert(`Failed to delete topic: ${err}`);
    }
  };

  return (
    <section>
      <header style={{ marginBottom: '40px', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <h1 style={{ fontSize: '32px', fontWeight: 600 }}>Topics Management</h1>
          <p style={{ color: 'var(--text-secondary)' }}>View and manage Kafka topics and partitions</p>
        </div>
        <button 
          onClick={() => setShowCreateModal(true)}
          style={{ 
            background: 'var(--accent-color)', 
            color: 'white', 
            padding: '10px 20px', 
            borderRadius: '12px', 
            display: 'flex', 
            alignItems: 'center', 
            gap: '8px',
            fontWeight: 600
          }}
        >
          <Plus size={20} /> Create Topic
        </button>
      </header>
      
      {showCreateModal && (
        <div style={{ 
          position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, 
          background: 'rgba(0,0,0,0.8)', backdropFilter: 'blur(4px)', 
          display: 'grid', placeItems: 'center', zIndex: 1000 
        }}>
          <div className="glass" style={{ width: '400px', padding: '32px', position: 'relative' }}>
            <button 
              onClick={() => setShowCreateModal(false)}
              style={{ position: 'absolute', top: '16px', right: '16px', color: 'var(--text-secondary)' }}
            >
              <X size={20} />
            </button>
            <h2 style={{ fontSize: '20px', marginBottom: '24px' }}>Create New Topic</h2>
            <form onSubmit={handleCreateTopic} style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
              <div>
                <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>TOPIC NAME</label>
                <input 
                  autoFocus
                  type="text" 
                  value={newTopicName}
                  onChange={(e) => setNewTopicName(e.target.value)}
                  placeholder="e.g. system.events"
                  style={{ width: '100%', background: 'rgba(255,255,255,0.05)', border: '1px solid var(--border-color)', color: 'white', padding: '12px', borderRadius: '8px' }}
                />
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>PARTITIONS</label>
                <input 
                  type="number" 
                  value={newTopicPartitions}
                  onChange={(e) => setNewTopicPartitions(parseInt(e.target.value))}
                  min={1}
                  max={20}
                  style={{ width: '100%', background: 'rgba(255,255,255,0.05)', border: '1px solid var(--border-color)', color: 'white', padding: '12px', borderRadius: '8px' }}
                />
              </div>
              {error && <div style={{ color: 'var(--danger)', fontSize: '13px' }}>{error}</div>}
              <button 
                type="submit"
                disabled={loading || !newTopicName}
                style={{ 
                  background: 'var(--accent-color)', 
                  color: 'white', 
                  padding: '12px', 
                  borderRadius: '12px', 
                  fontWeight: 600,
                  marginTop: '12px',
                  textAlign: 'center',
                  opacity: (loading || !newTopicName) ? 0.5 : 1
                }}
              >
                {loading ? 'Creating...' : 'Create Topic'}
              </button>
            </form>
          </div>
        </div>
      )}

      <div className="glass topic-list">
        {topics.map(t => (
          <div key={t.name} className="topic-row">
            <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
              <div className="glass" style={{ width: '40px', height: '40px', display: 'grid', placeItems: 'center', borderRadius: '12px' }}>
                <Box size={20} color="var(--text-secondary)" />
              </div>
              <div>
                <div style={{ fontWeight: 600 }}>{t.name}</div>
                <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
                  {t.partitions.length} Partitions • Replication: {t.partitions[0]?.replicas.length || 3}
                </div>
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
              <div className="status-badge status-online">HEALTHY</div>
              <button onClick={() => setSettingsTopic(t)} style={{ color: 'rgba(255,255,255,0.3)' }} className="hover-accent">
                <Settings size={18} />
              </button>
              <button 
                onClick={() => handleDeleteTopic(t.name)}
                style={{ color: 'var(--text-secondary)', padding: '8px', borderRadius: '8px' }}
                className="delete-btn"
                title="Delete Topic"
              >
                <Trash2 size={18} />
              </button>
            </div>
          </div>
        ))}
        {topics.length === 0 && (
          <div style={{ padding: '60px', textAlign: 'center', color: 'var(--text-secondary)', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '16px' }}>
            <Info size={48} style={{ opacity: 0.2 }} />
            <div>
              <div style={{ fontSize: '18px', fontWeight: 600, color: 'var(--text-primary)' }}>No topics detected</div>
              <p>Create your first topic to start streaming data.</p>
            </div>
          </div>
        )}
      </div>

      <style>{`
        .delete-btn:hover {
          color: var(--danger) !important;
          background: rgba(239, 68, 68, 0.1) !important;
        }
      `}</style>
      {settingsTopic && (
         <div className="modal-overlay">
           <div className="modal-content glass" style={{ width: '500px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '24px' }}>
                <Settings size={24} color="var(--accent-color)" />
                <h3 style={{ fontSize: '20px' }}>Settings: {settingsTopic.name}</h3>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
                <div>
                  <label style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '13px', color: 'var(--text-secondary)', marginBottom: '12px' }}>
                    <Clock size={16} /> DATA RETENTION
                  </label>
                  <select defaultValue="week" style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', padding: '12px', borderRadius: '8px', color: 'white' }}>
                    <option value="hour">1 Hour</option>
                    <option value="day">24 Hours</option>
                    <option value="week">7 Days</option>
                    <option value="forever">Forever</option>
                  </select>
                </div>
                <div>
                   <label style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '13px', color: 'var(--text-secondary)', marginBottom: '12px' }}>
                     <Database size={16} /> MAX SEGMENT SIZE
                   </label>
                   <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                     <input type="range" min="1" max="100" defaultValue="50" style={{ flex: 1 }} />
                     <span style={{ fontSize: '14px', width: '50px' }}>50MB</span>
                   </div>
                </div>
                <div>
                   <label style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '13px', color: 'var(--text-secondary)', marginBottom: '12px' }}>
                     <AlertCircle size={16} /> CLEANUP POLICY
                   </label>
                   <div style={{ display: 'flex', gap: '12px' }}>
                      <button style={{ flex: 1, padding: '10px', borderRadius: '8px', border: '1px solid var(--accent-color)', background: 'rgba(99, 102, 241, 0.1)', color: 'white', fontWeight: 600 }}>Delete</button>
                      <button style={{ flex: 1, padding: '10px', borderRadius: '8px', border: '1px solid var(--border-color)', background: 'transparent', color: 'var(--text-secondary)' }}>Compact</button>
                   </div>
                </div>
              </div>
              <div style={{ display: 'flex', gap: '12px', justifyContent: 'flex-end', marginTop: '32px' }}>
                <button onClick={() => setSettingsTopic(null)} style={{ padding: '10px 20px', borderRadius: '8px', color: 'var(--text-secondary)', fontWeight: 600 }}>Cancel</button>
                <button onClick={() => setSettingsTopic(null)} style={{ background: 'var(--accent-color)', color: 'white', padding: '10px 20px', borderRadius: '8px', fontWeight: 600, display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <Save size={18} /> Save Changes
                </button>
              </div>
           </div>
         </div>
      )}

      <style>{`
        .hover-accent:hover {
          color: var(--accent-color) !important;
        }
      `}</style>
    </section>
  );
};

export default Topics;
