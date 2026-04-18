import React, { useState, useEffect } from 'react';
import { LayoutDashboard, Database, Activity, ShieldCheck, Box, HardDrive, Cpu } from 'lucide-react';

interface BrokerMetadata {
  id: number;
  addr: string;
}

interface TopicInfo {
  name: string;
  partitions: number;
}

const App = () => {
  const [metadata, setMetadata] = useState<BrokerMetadata[]>([]);
  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [metrics, setMetrics] = useState<string>('');
  const [activeTab, setActiveTab] = useState('dashboard');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [metaRes, topicRes, metricsRes] = await Promise.all([
          fetch('/api/metadata'),
          fetch('/api/topics'),
          fetch('/api/metrics')
        ]);
        
        setMetadata(await metaRes.json());
        setTopics(await topicRes.json());
        setMetrics(await metricsRes.text());
      } catch (err) {
        console.error("Failed to fetch cluster data:", err);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  const parseMetric = (name: string) => {
    const match = metrics.match(new RegExp(`^${name}\\s+(\\d+)`, 'm'));
    return match ? match[1] : '0';
  };

  return (
    <div className="dashboard-container">
      <div className="sidebar">
        <div className="logo">
          <Box size={28} />
          <span>UIKAFKA</span>
        </div>
        
        <nav style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
          <button onClick={() => setActiveTab('dashboard')} className={`nav-item ${activeTab === 'dashboard' ? 'active' : ''}`}>
            <LayoutDashboard size={20} /> Dashboard
          </button>
          <button onClick={() => setActiveTab('topics')} className={`nav-item ${activeTab === 'topics' ? 'active' : ''}`}>
            <Database size={20} /> Topics
          </button>
          <button onClick={() => setActiveTab('system')} className={`nav-item ${activeTab === 'system' ? 'active' : ''}`}>
            <Activity size={20} /> System
          </button>
          <button onClick={() => setActiveTab('security')} className={`nav-item ${activeTab === 'security' ? 'active' : ''}`}>
            <ShieldCheck size={20} /> Security
          </button>
        </nav>

        <div style={{ marginTop: 'auto', padding: '16px', borderTop: '1px solid var(--border-color)' }}>
          <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>CLUSTER STATUS</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginTop: '8px' }}>
            <div style={{ width: '8px', height: '8px', background: 'var(--success)', borderRadius: '50%' }}></div>
            <span style={{ fontSize: '14px' }}>{metadata.length} Brokers Online</span>
          </div>
        </div>
      </div>

      <div className="main-content">
        <header style={{ marginBottom: '40px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h1 style={{ fontSize: '32px', fontWeight: 600 }}>Cluster Overview</h1>
            <p style={{ color: 'var(--text-secondary)' }}>Real-time telemetry from minimal_Kafaka cluster</p>
          </div>
          <div className="glass" style={{ padding: '8px 16px', display: 'flex', gap: '24px' }}>
             <div style={{ textAlign: 'right' }}>
               <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>CPU</div>
               <div style={{ fontWeight: 600 }}>12%</div>
             </div>
             <div style={{ textAlign: 'right' }}>
               <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>MEM</div>
               <div style={{ fontWeight: 600 }}>244MB</div>
             </div>
          </div>
        </header>

        <div className="metrics-grid">
           <div className="glass metric-card">
              <div className="metric-label">Messages In</div>
              <div className="metric-value" style={{ color: 'var(--accent-color)' }}>{parseMetric('gokafka_produce_total')}</div>
              <div style={{ fontSize: '12px', color: 'var(--success)' }}>+12% from last min</div>
           </div>
           <div className="glass metric-card">
              <div className="metric-label">Messages Out</div>
              <div className="metric-value">{parseMetric('gokafka_consume_total')}</div>
              <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>Active streams: 4</div>
           </div>
           <div className="glass metric-card">
              <div className="metric-label">Disk Storage</div>
              <div className="metric-value">{(parseInt(parseMetric('gokafka_storage_bytes')) / 1024).toFixed(2)} KB</div>
              <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>Log Compaction: Enabled</div>
           </div>
        </div>

        <section>
          <h2 style={{ fontSize: '20px', marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Database size={20} /> Active Topics
          </h2>
          <div className="glass topic-list">
            {topics.map(t => (
              <div key={t.name} className="topic-row">
                <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
                  <div className="glass" style={{ width: '40px', height: '40px', display: 'grid', placeItems: 'center', borderRadius: '12px' }}>
                    <Box size={20} color="var(--text-secondary)" />
                  </div>
                  <div>
                    <div style={{ fontWeight: 600 }}>{t.name}</div>
                    <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>{t.partitions} Partitions • Replication: 3</div>
                  </div>
                </div>
                <div className="status-badge status-online">HEALTHY</div>
              </div>
            ))}
            {topics.length === 0 && (
              <div style={{ padding: '40px', textAlign: 'center', color: 'var(--text-secondary)' }}>
                No topics detected in the cluster.
              </div>
            )}
          </div>
        </section>
      </div>
    </div>
  );
};

export default App;
