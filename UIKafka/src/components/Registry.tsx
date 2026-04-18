import React, { useState, useEffect } from 'react';
import { Book, FileJson, Plus, Search, ChevronRight, FileCode } from 'lucide-react';

interface Schema {
  id: number;
  subject: string;
  version: number;
  schema: string;
}

interface RegistryProps {
  currentUser: string;
}

const Registry: React.FC<RegistryProps> = ({ currentUser }) => {
  const [subjects, setSubjects] = useState<string[]>([]);
  const [selectedSubject, setSelectedSubject] = useState<string | null>(null);
  const [schemaData, setSchemaData] = useState<Schema | null>(null);
  const [newSchema, setNewSchema] = useState({ subject: '', schema: '' });
  const [isAdding, setIsAdding] = useState(false);
  const [loading, setLoading] = useState(true);

  const fetchSubjects = async () => {
    try {
      const res = await fetch('/api/subjects', {
        headers: { 'X-User': currentUser }
      });
      if (res.ok) {
        setSubjects(await res.json());
      }
    } catch (err) {
      console.error("Failed to fetch subjects:", err);
    } finally {
      setLoading(false);
    }
  };

  const fetchSchema = async (subject: string) => {
    try {
      const res = await fetch(`/api/subjects?subject=${subject}`, {
        headers: { 'X-User': currentUser }
      });
      if (res.ok) {
        setSchemaData(await res.json());
        setSelectedSubject(subject);
      }
    } catch (err) {
      console.error("Failed to fetch schema:", err);
    }
  };

  useEffect(() => {
    fetchSubjects();
  }, []);

  const handleRegister = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const res = await fetch('/api/subjects', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'X-User': currentUser
        },
        body: JSON.stringify(newSchema)
      });
      if (res.ok) {
        setNewSchema({ subject: '', schema: '' });
        setIsAdding(false);
        fetchSubjects();
      }
    } catch (err) {
      console.error("Failed to register schema:", err);
    }
  };

  return (
    <section>
      <header style={{ marginBottom: '40px', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end' }}>
        <div>
          <h1 style={{ fontSize: '32px', fontWeight: 600 }}>Schema Registry</h1>
          <p style={{ color: 'var(--text-secondary)' }}>Manage data contracts and schema versions</p>
        </div>
        <button 
          onClick={() => setIsAdding(true)}
          style={{ background: 'var(--accent-color)', color: 'white', padding: '10px 20px', borderRadius: '8px', fontWeight: 600, display: 'flex', alignItems: 'center', gap: '8px' }}
        >
          <Plus size={18} /> Register Schema
        </button>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: '300px 1fr', gap: '24px', height: 'calc(100vh - 250px)' }}>
        <div className="glass" style={{ padding: '0', display: 'flex', flexDirection: 'column' }}>
          <div style={{ padding: '16px', borderBottom: '1px solid var(--border-color)' }}>
             <div style={{ position: 'relative' }}>
               <Search size={14} style={{ position: 'absolute', left: '12px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-secondary)' }} />
               <input 
                 type="text" 
                 placeholder="Search subjects..." 
                 style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', borderRadius: '8px', padding: '8px 8px 8px 32px', color: 'white', fontSize: '13px' }}
               />
             </div>
          </div>
          <div style={{ flex: 1, overflowY: 'auto' }}>
            {subjects.map(s => (
              <div 
                key={s} 
                onClick={() => fetchSchema(s)}
                style={{ 
                  padding: '16px', borderBottom: '1px solid var(--border-color)', cursor: 'pointer',
                  background: selectedSubject === s ? 'rgba(255,255,255,0.05)' : 'transparent',
                  display: 'flex', justifyContent: 'space-between', alignItems: 'center'
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                  <Book size={16} color={selectedSubject === s ? 'var(--accent-color)' : 'var(--text-secondary)'} />
                  <span style={{ fontSize: '14px', fontWeight: selectedSubject === s ? 600 : 400 }}>{s}</span>
                </div>
                <ChevronRight size={14} color="var(--text-secondary)" />
              </div>
            ))}
            {subjects.length === 0 && !loading && (
               <div style={{ padding: '40px', textAlign: 'center', color: 'var(--text-secondary)', fontSize: '13px' }}>
                 No subjects registered
               </div>
            )}
          </div>
        </div>

        <div className="glass" style={{ padding: '24px', display: 'flex', flexDirection: 'column' }}>
          {selectedSubject && schemaData ? (
             <>
               <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '24px' }}>
                 <div>
                   <h2 style={{ fontSize: '20px', fontWeight: 600 }}>{selectedSubject}</h2>
                   <div style={{ display: 'flex', gap: '12px', marginTop: '8px' }}>
                     <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>ID: {schemaData.id}</span>
                     <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>Version: {schemaData.version}</span>
                   </div>
                 </div>
                 <span className="status-badge status-online">LATEST</span>
               </div>

               <div style={{ flex: 1, background: 'rgba(0,0,0,0.3)', borderRadius: '8px', padding: '20px', border: '1px solid var(--border-color)', overflowY: 'auto' }}>
                 <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px', color: 'var(--accent-color)', fontSize: '13px', fontWeight: 600 }}>
                   <FileCode size={16} /> JSON SCHEMA
                 </div>
                 <pre style={{ margin: 0, fontSize: '13px', color: '#888', whiteSpace: 'pre-wrap' }}>
                   {schemaData.schema}
                 </pre>
               </div>
             </>
          ) : (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', color: 'var(--text-secondary)' }}>
              <FileJson size={48} style={{ marginBottom: '16px', opacity: 0.2 }} />
              <p>Select a subject to view its schema details</p>
            </div>
          )}
        </div>
      </div>

      {isAdding && (
        <div className="modal-overlay">
          <div className="modal-content glass" style={{ width: '600px' }}>
            <h3 style={{ fontSize: '20px', marginBottom: '24px' }}>Register New Schema</h3>
            <form onSubmit={handleRegister} style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
              <div>
                <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>SUBJECT NAME</label>
                <input 
                  type="text" 
                  value={newSchema.subject}
                  onChange={e => setNewSchema({...newSchema, subject: e.target.value})}
                  placeholder="e.g. user-events-value"
                  style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', padding: '12px', borderRadius: '8px', color: 'white' }}
                />
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '12px', color: 'var(--text-secondary)', marginBottom: '8px' }}>SCHEMA DEFINITION (JSON)</label>
                <textarea 
                  value={newSchema.schema}
                  onChange={e => setNewSchema({...newSchema, schema: e.target.value})}
                  placeholder='{ "type": "record", ... }'
                  rows={10}
                  style={{ width: '100%', background: 'rgba(0,0,0,0.2)', border: '1px solid var(--border-color)', padding: '12px', borderRadius: '8px', color: 'white', fontFamily: 'monospace' }}
                />
              </div>
              <div style={{ display: 'flex', gap: '12px', justifyContent: 'flex-end', marginTop: '12px' }}>
                <button type="button" onClick={() => setIsAdding(false)} style={{ padding: '10px 20px', borderRadius: '8px', color: 'var(--text-secondary)', fontWeight: 600 }}>Cancel</button>
                <button type="submit" style={{ background: 'var(--accent-color)', color: 'white', padding: '10px 20px', borderRadius: '8px', fontWeight: 600 }}>Register</button>
              </div>
            </form>
          </div>
        </div>
      )}
    </section>
  );
};

export default Registry;
