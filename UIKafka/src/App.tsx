import React, { useState, useEffect, useCallback } from 'react';
import { LayoutDashboard, Database, Activity, ShieldCheck, Box, MessageSquare, Users, Book, User } from 'lucide-react';
import Dashboard from './components/Dashboard';
import Topics from './components/Topics';
import System from './components/System';
import Security from './components/Security';
import Interact from './components/Interact';
import Groups from './components/Groups';
import Registry from './components/Registry';

interface BrokerMetadata {
  id: number;
  addr: string;
}

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

const App = () => {
  const [metadata, setMetadata] = useState<BrokerMetadata[]>([]);
  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [metrics, setMetrics] = useState<string>('');
  const [activeTab, setActiveTab] = useState('dashboard');
  const [currentUser, setCurrentUser] = useState(localStorage.getItem('kafka-user') || 'admin');

  useEffect(() => {
    localStorage.setItem('kafka-user', currentUser);
  }, [currentUser]);

  const fetchData = useCallback(async () => {
    try {
      const [metaRes, metricsRes] = await Promise.all([
        fetch('/api/metadata', { headers: { 'X-User': currentUser } }),
        fetch('/api/metrics', { headers: { 'X-User': currentUser } })
      ]);
      
      const metaData = await metaRes.json();
      
      const brokerList = Object.entries(metaData.brokers || {}).map(([id, addr]) => ({
        id: parseInt(id),
        addr: addr as string
      }));
      
      const topicList = Object.values(metaData.topics || {}).map((t: any) => ({
        name: t.name,
        partitions: t.partitions.map((p: any) => ({
          id: p.id,
          leader: p.leader,
          replicas: p.replicas || [],
          isr: p.isr || []
        }))
      }));

      setMetadata(brokerList);
      setTopics(topicList);
      setMetrics(await metricsRes.text());
    } catch (err) {
      console.error("Failed to fetch cluster data:", err);
    }
  }, []);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const renderContent = () => {
    switch (activeTab) {
      case 'dashboard':
        return <Dashboard metrics={metrics} topics={topics} brokers={metadata} />;
      case 'topics':
        return <Topics topics={topics} onRefresh={fetchData} currentUser={currentUser} />;
      case 'interact':
        return <Interact topics={topics} currentUser={currentUser} />;
      case 'groups':
        return <Groups currentUser={currentUser} />;
      case 'registry':
        return <Registry currentUser={currentUser} />;
      case 'system':
        return <System brokers={metadata} topics={topics} />;
      case 'security':
        return <Security topics={topics} currentUser={currentUser} />;
      default:
        return <Dashboard metrics={metrics} topics={topics} brokers={metadata} />;
    }
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
          <button onClick={() => setActiveTab('interact')} className={`nav-item ${activeTab === 'interact' ? 'active' : ''}`}>
            <MessageSquare size={20} /> Interact
          </button>
          <button onClick={() => setActiveTab('groups')} className={`nav-item ${activeTab === 'groups' ? 'active' : ''}`}>
            <Users size={20} /> Groups
          </button>
          <button onClick={() => setActiveTab('registry')} className={`nav-item ${activeTab === 'registry' ? 'active' : ''}`}>
            <Book size={20} /> Registry
          </button>
          <button onClick={() => setActiveTab('system')} className={`nav-item ${activeTab === 'system' ? 'active' : ''}`}>
            <Activity size={20} /> System
          </button>
          <button onClick={() => setActiveTab('security')} className={`nav-item ${activeTab === 'security' ? 'active' : ''}`}>
            <ShieldCheck size={20} /> Security
          </button>
        </nav>

        <div style={{ padding: '24px', background: 'rgba(255,255,255,0.02)', borderRadius: '12px', margin: '16px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '12px' }}>
            <User size={14} /> ACTIVE PRINCIPAL
          </div>
          <input 
            type="text" 
            value={currentUser} 
            onChange={(e) => setCurrentUser(e.target.value)}
            style={{ 
              width: '100%', 
              background: 'rgba(0,0,0,0.2)', 
              border: '1px solid var(--border-color)', 
              borderRadius: '8px', 
              padding: '8px 12px', 
              color: 'white',
              fontSize: '13px',
              outline: 'none'
            }}
          />
        </div>

        <div style={{ marginTop: 'auto', padding: '16px', borderTop: '1px solid var(--border-color)' }}>
          <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>CLUSTER STATUS</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginTop: '8px' }}>
            <div style={{ width: '8px', height: '8px', background: 'var(--success)', borderRadius: '50%' }}></div>
            <span style={{ fontSize: '14px' }}>{metadata.length} Brokers Online</span>
          </div>
        </div>
      </div>

      <div className="main-content">
        {renderContent()}
      </div>
    </div>
  );
};

export default App;
