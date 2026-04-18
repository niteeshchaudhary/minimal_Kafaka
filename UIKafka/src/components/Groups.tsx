import React, { useState, useEffect } from 'react';
import { Users, AlertCircle, ChevronDown, ChevronRight, Hash } from 'lucide-react';

interface GroupInfo {
  name: string;
  topic: string;
  members: string[];
  total_lag: number;
  partition_lag: { [key: number]: number };
}

interface GroupsProps {
  currentUser: string;
}

const Groups: React.FC<GroupsProps> = ({ currentUser }) => {
  const [groups, setGroups] = useState<GroupInfo[]>([]);
  const [expandedGroup, setExpandedGroup] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchGroups = async () => {
    try {
      const res = await fetch('/api/groups', {
        headers: { 'X-User': currentUser }
      });
      if (res.ok) {
        setGroups(await res.json());
      }
    } catch (err) {
      console.error("Failed to fetch groups:", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchGroups();
    const interval = setInterval(fetchGroups, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <section>
      <header style={{ marginBottom: '40px' }}>
        <h1 style={{ fontSize: '32px', fontWeight: 600 }}>Consumer Groups</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Monitor consumer lag and partition assignments</p>
      </header>

      <div className="glass" style={{ padding: '0' }}>
        <div style={{ padding: '24px', borderBottom: '1px solid var(--border-color)', display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr 100px', fontSize: '12px', fontWeight: 600, color: 'var(--text-secondary)' }}>
          <span>GROUP NAME</span>
          <span>TOPIC</span>
          <span>MEMBERS</span>
          <span>TOTAL LAG</span>
          <span style={{ textAlign: 'right' }}>STATUS</span>
        </div>

        {groups.map(group => (
          <div key={group.name} style={{ borderBottom: '1px solid var(--border-color)' }}>
            <div 
              onClick={() => setExpandedGroup(expandedGroup === group.name ? null : group.name)}
              style={{ padding: '24px', display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr 100px', alignItems: 'center', cursor: 'pointer', transition: 'background 0.2s' }}
              className="group-row"
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 600 }}>
                {expandedGroup === group.name ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                <Users size={18} color="var(--accent-color)" />
                {group.name}
              </div>
              <div style={{ color: 'var(--text-secondary)' }}>{group.topic || 'N/A'}</div>
              <div>
                <span style={{ background: 'rgba(255,255,255,0.05)', padding: '4px 8px', borderRadius: '4px', fontSize: '12px' }}>
                  {group.members.length} Members
                </span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ color: group.total_lag > 100 ? 'var(--danger)' : group.total_lag > 0 ? 'var(--warning)' : 'var(--success)', fontWeight: 600 }}>
                  {group.total_lag}
                </span>
                {group.total_lag > 100 && <AlertCircle size={14} color="var(--danger)" />}
              </div>
              <div style={{ textAlign: 'right' }}>
                <div className={`status-badge ${group.members.length > 0 ? 'status-online' : ''}`} style={{ fontSize: '10px' }}>
                  {group.members.length > 0 ? 'STABLE' : 'EMPTY'}
                </div>
              </div>
            </div>

            {expandedGroup === group.name && (
              <div style={{ padding: '0 24px 24px 64px', background: 'rgba(255,255,255,0.01)' }}>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '40px' }}>
                  <div>
                    <h4 style={{ fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '16px', display: 'flex', alignItems: 'center', gap: '6px' }}>
                      <Hash size={14} /> PARTITION LAG
                    </h4>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                      {Object.entries(group.partition_lag).map(([id, lag]) => (
                        <div key={id} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', padding: '8px 12px', background: 'rgba(0,0,0,0.2)', borderRadius: '8px' }}>
                          <span>Partition {id}</span>
                          <span style={{ fontWeight: 600, color: lag > 0 ? 'var(--warning)' : 'inherit' }}>{lag}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div>
                    <h4 style={{ fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '16px' }}>MEMBERS</h4>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                      {group.members.map(member => (
                        <div key={member} style={{ fontSize: '13px', padding: '8px 12px', background: 'rgba(0,0,0,0.2)', borderRadius: '8px', color: 'var(--text-secondary)' }}>
                          {member}
                        </div>
                      ))}
                      {group.members.length === 0 && <div style={{ fontSize: '13px', color: 'var(--text-secondary)', fontStyle: 'italic' }}>No active members</div>}
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        ))}

        {groups.length === 0 && !loading && (
          <div style={{ padding: '60px', textAlign: 'center', color: 'var(--text-secondary)' }}>
            No consumer groups detected. Start a consumer to see group activity.
          </div>
        )}
      </div>

      <style>{`
        .group-row:hover {
          background: rgba(255,255,255,0.03);
        }
      `}</style>
    </section>
  );
};

export default Groups;
