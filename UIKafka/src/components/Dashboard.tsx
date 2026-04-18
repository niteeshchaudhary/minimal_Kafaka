import React from 'react';
import { AlertTriangle, Activity, Database, Server } from 'lucide-react';

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

interface BrokerMetadata {
  id: number;
  addr: string;
}

interface DashboardProps {
  metrics: string;
  topics: TopicInfo[];
  brokers: BrokerMetadata[];
}

const Dashboard: React.FC<DashboardProps> = ({ metrics, topics, brokers }) => {
  const parseMetric = (name: string) => {
    const match = metrics.match(new RegExp(`^${name}\\s+(\\d+)`, 'm'));
    return match ? match[1] : '0';
  };

  const urpCount = topics.reduce((acc, topic) => {
    return acc + topic.partitions.filter(p => p.isr.length < p.replicas.length).length;
  }, 0);

  return (
    <>
      <header style={{ marginBottom: '40px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <h1 style={{ fontSize: '32px', fontWeight: 600 }}>Cluster Overview</h1>
          <p style={{ color: 'var(--text-secondary)' }}>Real-time telemetry from gokafka cluster</p>
        </div>
        <div className="glass" style={{ padding: '8px 16px', display: 'flex', gap: '24px' }}>
           <div style={{ textAlign: 'right' }}>
             <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>TOTAL TOPICS</div>
             <div style={{ fontWeight: 600 }}>{topics.length}</div>
           </div>
           <div style={{ textAlign: 'right' }}>
             <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>PARTITIONS</div>
             <div style={{ fontWeight: 600 }}>{topics.reduce((a, t) => a + t.partitions.length, 0)}</div>
           </div>
        </div>
      </header>

      {urpCount > 0 && (
        <div className="glass" style={{ marginBottom: '32px', padding: '16px 24px', borderLeft: '4px solid var(--danger)', display: 'flex', alignItems: 'center', gap: '16px' }}>
          <AlertTriangle color="var(--danger)" />
          <div>
            <div style={{ fontWeight: 600, color: 'var(--danger)' }}>UNDER-REPLICATED PARTITIONS DETECTED</div>
            <div style={{ fontSize: '14px', color: 'var(--text-secondary)' }}>{urpCount} partitions are currently lagging behind their leaders.</div>
          </div>
        </div>
      )}

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
            <div className="metric-label">URP Count</div>
            <div className="metric-value" style={{ color: urpCount > 0 ? 'var(--danger)' : 'inherit' }}>{urpCount}</div>
            <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>Replication Consistency</div>
         </div>
         <div className="glass metric-card">
            <div className="metric-label">Disk Storage</div>
            <div className="metric-value">{(parseInt(parseMetric('gokafka_storage_bytes')) / 1024).toFixed(2)} KB</div>
            <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>Log Compaction: Enabled</div>
         </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
        <div className="glass" style={{ padding: '24px' }}>
          <h2 style={{ fontSize: '18px', marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Activity size={18} /> Throughput (Msg/sec)
          </h2>
          <div style={{ height: '200px', display: 'flex', alignItems: 'flex-end', gap: '8px', padding: '0 8px' }}>
            {[40, 65, 55, 80, 45, 90, 70, 85, 60, 75].map((h, i) => (
              <div key={i} style={{ flex: 1, background: 'var(--accent-color)', height: `${h}%`, borderRadius: '4px 4px 0 0', opacity: 0.3 + (i * 0.07) }}></div>
            ))}
          </div>
        </div>
        <div className="glass" style={{ padding: '24px' }}>
          <h2 style={{ fontSize: '18px', marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Server size={18} /> Leader Distribution
          </h2>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
             {/* Simple visualization of leader count per broker */}
             {[1, 2, 3].map(id => {
               const count = topics.reduce((acc, t) => acc + t.partitions.filter(p => p.leader === id).length, 0);
               const total = topics.reduce((acc, t) => acc + t.partitions.length, 0) || 1;
               return (
                 <div key={id}>
                   <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px', marginBottom: '4px' }}>
                     <span>Broker {id}</span>
                     <span>{count} Leaders</span>
                   </div>
                   <div style={{ width: '100%', height: '8px', background: 'rgba(255,255,255,0.05)', borderRadius: '4px', overflow: 'hidden' }}>
                     <div style={{ height: '100%', width: `${(count/total)*100}%`, background: 'var(--accent-color)' }}></div>
                   </div>
                 </div>
               );
             })}
          </div>
        </div>
      </div>

      <div className="glass" style={{ padding: '24px', marginTop: '24px' }}>
        <h3 style={{ fontSize: '18px', marginBottom: '24px', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Activity size={18} /> Cluster Topology
        </h3>
        <div style={{ height: '300px', display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
          <svg width="100%" height="100%" viewBox="0 0 800 300">
            {/* Connection Lines */}
            {brokers.map((b, index) => (
              <line 
                key={`line-${b.id}`}
                x1="400" y1="150" 
                x2={150 + index * 250} y2="220" 
                stroke="var(--accent-color)" 
                strokeWidth="1" 
                strokeDasharray="4 4"
                opacity="0.3"
              />
            ))}
            {topics.slice(0, 3).map((topic, index) => (
              <line 
                key={`topic-line-${index}`}
                x1="400" y1="150" 
                x2={100 + index * 300} y2="80" 
                stroke="var(--success)" 
                strokeWidth="1" 
                opacity="0.2"
              />
            ))}

            {/* Cluster Center */}
            <circle cx="400" cy="150" r="40" fill="rgba(99, 102, 241, 0.1)" stroke="var(--accent-color)" strokeWidth="2" />
            <text x="400" y="155" textAnchor="middle" fill="white" fontSize="12" fontWeight="600">CLUSTER</text>

            {/* Brokers */}
            {brokers.map((b, index) => (
              <g key={`broker-node-${b.id}`} transform={`translate(${150 + index * 250}, 220)`}>
                <rect x="-60" y="-20" width="120" height="40" rx="8" fill="rgba(0,0,0,0.4)" stroke="var(--accent-color)" strokeWidth="1" />
                <text x="0" y="5" textAnchor="middle" fill="white" fontSize="11">Broker {b.id}</text>
              </g>
            ))}

            {/* Topics */}
            {topics.slice(0, 3).map((topic, index) => (
              <g key={`topic-node-${index}`} transform={`translate(${100 + index * 300}, 80)`}>
                <rect x="-50" y="-15" width="100" height="30" rx="15" fill="rgba(16, 185, 129, 0.1)" stroke="var(--success)" strokeWidth="1" />
                <text x="0" y="5" textAnchor="middle" fill="var(--success)" fontSize="10">{topic.name}</text>
              </g>
            ))}
          </svg>
        </div>
      </div>
    </>
  );
};

export default Dashboard;
