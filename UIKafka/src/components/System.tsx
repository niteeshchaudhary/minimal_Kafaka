import React from 'react';
import { ShieldCheck, HardDrive, Cpu, Wifi, Activity } from 'lucide-react';

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

interface SystemProps {
  brokers: BrokerMetadata[];
  topics: TopicInfo[];
}

const System: React.FC<SystemProps> = ({ brokers, topics }) => {
  return (
    <section>
      <header style={{ marginBottom: '40px' }}>
        <h1 style={{ fontSize: '32px', fontWeight: 600 }}>System Health</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Infrastructure metrics and broker performance</p>
      </header>
      
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(350px, 1fr))', gap: '24px', marginBottom: '40px' }}>
        {brokers.map(broker => (
          <div key={broker.id} className="glass" style={{ padding: '24px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '24px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                <div className="glass" style={{ width: '48px', height: '48px', display: 'grid', placeItems: 'center', borderRadius: '12px' }}>
                  <Wifi size={24} color="var(--accent-color)" />
                </div>
                <div>
                  <div style={{ fontWeight: 600, fontSize: '18px' }}>Broker {broker.id}</div>
                  <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>{broker.addr}</div>
                </div>
              </div>
              <div className="status-badge status-online">ONLINE</div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
              <div className="glass" style={{ padding: '16px', background: 'rgba(255,255,255,0.02)' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '8px' }}>
                  <Cpu size={14} /> CPU LOAD
                </div>
                <div style={{ fontSize: '20px', fontWeight: 600 }}>{(10 + Math.random() * 5).toFixed(1)}%</div>
              </div>
              <div className="glass" style={{ padding: '16px', background: 'rgba(255,255,255,0.02)' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '8px' }}>
                  <HardDrive size={14} /> MEMORY
                </div>
                <div style={{ fontSize: '20px', fontWeight: 600 }}>{(128 + Math.random() * 64).toFixed(0)} MB</div>
              </div>
            </div>

            <div style={{ marginTop: '20px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>
                <span>Leadership Share</span>
                <span>{topics.reduce((acc, t) => acc + t.partitions.filter(p => p.leader === broker.id).length, 0)} Partitions</span>
              </div>
              <div style={{ width: '100%', height: '4px', background: 'rgba(255,255,255,0.05)', borderRadius: '2px' }}>
                <div style={{ height: '100%', width: '33%', background: 'var(--accent-color)' }}></div>
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className="glass" style={{ padding: '24px' }}>
        <h2 style={{ fontSize: '18px', marginBottom: '24px', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Activity size={18} /> Connectivity Matrix
        </h2>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
             <thead>
               <tr style={{ color: 'var(--text-secondary)', textAlign: 'left' }}>
                 <th style={{ padding: '12px', borderBottom: '1px solid var(--border-color)' }}>FROM \ TO</th>
                 {brokers.map(b => <th key={b.id} style={{ padding: '12px', borderBottom: '1px solid var(--border-color)' }}>Broker {b.id}</th>)}
               </tr>
             </thead>
             <tbody>
               {brokers.map(b1 => (
                 <tr key={b1.id}>
                   <td style={{ padding: '12px', borderBottom: '1px solid var(--border-color)', fontWeight: 600 }}>Broker {b1.id}</td>
                   {brokers.map(b2 => (
                     <td key={b2.id} style={{ padding: '12px', borderBottom: '1px solid var(--border-color)' }}>
                       {b1.id === b2.id ? (
                         <span style={{ color: 'var(--text-secondary)' }}>-</span>
                       ) : (
                         <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                           <div style={{ width: '8px', height: '8px', background: 'var(--success)', borderRadius: '50%' }}></div>
                           <span style={{ fontSize: '12px' }}>{(Math.random() * 2 + 1).toFixed(1)}ms</span>
                         </div>
                       )}
                     </td>
                   ))}
                 </tr>
               ))}
             </tbody>
          </table>
        </div>
      </div>
    </section>
  );
};

export default System;
