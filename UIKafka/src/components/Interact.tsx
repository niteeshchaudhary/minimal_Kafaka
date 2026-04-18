import React, { useState } from 'react';
import { Send, Download, Box, Terminal } from 'lucide-react';

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

interface Message {
  offset: number;
  timestamp: number;
  key: string;
  value: string;
}

interface InteractProps {
  topics: TopicInfo[];
  currentUser: string;
}

const Interact: React.FC<InteractProps> = ({ topics, currentUser }) => {
  const [selectedTopic, setSelectedTopic] = useState(topics.length > 0 ? topics[0].name : '');
  const [prodKey, setProdKey] = useState('');
  const [prodValue, setProdValue] = useState('');
  const [consPartition, setConsPartition] = useState(0);
  const [consOffset, setConsOffset] = useState(0);
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState({ prod: false, cons: false });
  const [status, setStatus] = useState({ prod: '', cons: '' });

  const handleProduce = async () => {
    if (!selectedTopic || !prodValue) return;
    setLoading(prev => ({ ...prev, prod: true }));
    try {
      const res = await fetch('/api/produce', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'X-User': currentUser
        },
        body: JSON.stringify({ topic: selectedTopic, key: prodKey, value: prodValue })
      });
      if (res.ok) {
        const data = await res.json();
        setStatus(prev => ({ ...prev, prod: `Message sent to partition ${data.partition} at offset ${data.offset}` }));
        setProdValue('');
      } else {
        const errorText = await res.text();
        setStatus(prev => ({ ...prev, prod: `Error: ${errorText}` }));
      }
    } catch (err) {
      setStatus(prev => ({ ...prev, prod: `Failed to connect: ${err}` }));
    } finally {
      setLoading(prev => ({ ...prev, prod: false }));
    }
  };

  const handleConsume = async () => {
    if (!selectedTopic) return;
    setLoading(prev => ({ ...prev, cons: true }));
    try {
      const res = await fetch(`/api/consume?topic=${selectedTopic}&partition=${consPartition}&offset=${consOffset}`, {
        headers: { 'X-User': currentUser }
      });
      if (res.ok) {
        const data = await res.json();
        setMessages(data || []);
        setStatus(prev => ({ ...prev, cons: `Fetched ${data?.length || 0} messages` }));
      } else {
        const errorText = await res.text();
        setStatus(prev => ({ ...prev, cons: `Error: ${errorText}` }));
      }
    } catch (err) {
      setStatus(prev => ({ ...prev, cons: `Failed to connect: ${err}` }));
    } finally {
      setLoading(prev => ({ ...prev, cons: false }));
    }
  };

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
      <header style={{ gridColumn: 'span 2', marginBottom: '16px' }}>
        <h1 style={{ fontSize: '32px', fontWeight: 600 }}>Message Interaction</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Produce and consume messages in real-time</p>
        
        <div className="glass" style={{ marginTop: '24px', padding: '16px', display: 'flex', alignItems: 'center', gap: '16px' }}>
          <Box size={20} color="var(--accent-color)" />
          <span style={{ fontWeight: 600 }}>Target Topic:</span>
          <select 
            value={selectedTopic} 
            onChange={(e) => setSelectedTopic(e.target.value)}
            style={{ 
              background: 'rgba(255,255,255,0.05)', 
              border: '1px solid var(--border-color)', 
              color: 'var(--text-primary)',
              padding: '8px 12px',
              borderRadius: '8px',
              flex: 1
            }}
          >
            <option value="">Select a topic...</option>
            {topics.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
          </select>
        </div>
      </header>

      {/* Produce Section */}
      <div className="glass" style={{ padding: '24px' }}>
        <h2 style={{ fontSize: '18px', marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Send size={18} /> Produce Message
        </h2>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          <div>
            <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>KEY (OPTIONAL)</label>
            <input 
              type="text" 
              value={prodKey}
              onChange={(e) => setProdKey(e.target.value)}
              placeholder="e.g. user-123"
              style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', color: 'white', padding: '12px', borderRadius: '8px' }}
            />
          </div>
          <div>
            <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>VALUE</label>
            <textarea 
              value={prodValue}
              onChange={(e) => setProdValue(e.target.value)}
              placeholder="Message payload..."
              rows={4}
              style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', color: 'white', padding: '12px', borderRadius: '8px', resize: 'vertical' }}
            />
          </div>
          <button 
            onClick={handleProduce}
            disabled={loading.prod || !selectedTopic}
            style={{ 
              background: 'var(--accent-color)', 
              color: 'white', 
              padding: '12px', 
              borderRadius: '8px', 
              textAlign: 'center',
              fontWeight: 600,
              opacity: (loading.prod || !selectedTopic) ? 0.5 : 1
            }}
          >
            {loading.prod ? 'Sending...' : 'Send Message'}
          </button>
          {status.prod && <div style={{ fontSize: '12px', color: status.prod.startsWith('Error') ? 'var(--danger)' : 'var(--success)' }}>{status.prod}</div>}
        </div>
      </div>

      {/* Consume Section */}
      <div className="glass" style={{ padding: '24px', display: 'flex', flexDirection: 'column' }}>
        <h2 style={{ fontSize: '18px', marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Download size={18} /> Consume Messages
        </h2>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px', marginBottom: '16px' }}>
          <div>
            <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>PARTITION</label>
            <input 
              type="number" 
              value={consPartition}
              onChange={(e) => setConsPartition(parseInt(e.target.value))}
              style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', color: 'white', padding: '12px', borderRadius: '8px' }}
            />
          </div>
          <div>
            <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>START OFFSET</label>
            <input 
              type="number" 
              value={consOffset}
              onChange={(e) => setConsOffset(parseInt(e.target.value))}
              style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', color: 'white', padding: '12px', borderRadius: '8px' }}
            />
          </div>
        </div>
        <button 
          onClick={handleConsume}
          disabled={loading.cons || !selectedTopic}
          style={{ 
            background: 'rgba(255,255,255,0.05)', 
            border: '1px solid var(--border-color)',
            color: 'white', 
            padding: '12px', 
            borderRadius: '8px', 
            textAlign: 'center',
            fontWeight: 600,
            marginBottom: '16px'
          }}
        >
          {loading.cons ? 'Fetching...' : 'Fetch Messages'}
        </button>
        
        <div style={{ flex: 1, minHeight: '200px', background: 'rgba(0,0,0,0.3)', borderRadius: '8px', padding: '12px', overflowY: 'auto' }}>
          {messages.length === 0 ? (
            <div style={{ height: '100%', display: 'grid', placeItems: 'center', color: 'var(--text-secondary)', fontSize: '14px' }}>
              <Terminal size={32} style={{ marginBottom: '8px', opacity: 0.5 }} />
              No messages to display.
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
              {messages.map((m, i) => (
                <div key={i} style={{ padding: '8px', borderBottom: '1px solid var(--border-color)', fontSize: '13px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--accent-color)', fontSize: '11px', marginBottom: '4px' }}>
                    <span>Offset: {m.offset !== undefined ? m.offset : 'N/A'}</span>
                    <span>{m.timestamp ? new Date(m.timestamp).toLocaleString() : 'No Timestamp'}</span>
                  </div>
                  <div style={{ color: 'var(--text-secondary)', marginBottom: '4px' }}>Key: <span style={{ color: 'var(--text-primary)' }}>{m.key || 'null'}</span></div>
                  <pre style={{ background: 'rgba(255,255,255,0.02)', padding: '8px', borderRadius: '4px', overflowX: 'auto' }}>{m.value || '(Empty)'}</pre>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Interact;
