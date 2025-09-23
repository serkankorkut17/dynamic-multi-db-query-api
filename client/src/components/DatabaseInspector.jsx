import { useState } from 'react';
import { postJson } from '../utils/api';

const DatabaseInspector = ({ config, data, onInspect }) => {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isVisible, setIsVisible] = useState(false);
  const [isCollapsed, setIsCollapsed] = useState(false);

  const handleInspect = async () => {
    if (!config.dbType || !config.connectionString) {
      setError('Lütfen database yapılandırmasını tamamlayın');
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const payload = {
        dbType: config.dbType,
        connectionString: config.connectionString
      };

      const response = await postJson('/api/query/inspect', payload);
      onInspect(response);
      setIsVisible(true);
    } catch (err) {
      setError(err.message || 'Database inceleme sırasında hata oluştu');
      setIsVisible(true);
    } finally {
      setIsLoading(false);
    }
  };

  const renderDatabaseStructure = () => {
    if (!data || !data.tables || !Array.isArray(data.tables)) {
      return <p>Tablo bilgisi bulunamadı</p>;
    }

    return (
      <div>
        <h4>{data.tables.length} tablo bulundu:</h4>
        {data.tables.map((table, index) => (
          <details key={index} className="details">
            <summary className="summary">
              {table.table} ({table.columns.length} kolon)
            </summary>
            <div style={{ marginTop: '0.5rem' }}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Kolon</th>
                    <th>Tip</th>
                    <th>Null</th>
                  </tr>
                </thead>
                <tbody>
                  {table.columns.map((col, colIndex) => (
                    <tr key={colIndex}>
                      <td>{col.name}</td>
                      <td>{col.dataType}</td>
                      <td>{col.isNullable ? 'Evet' : 'Hayır'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              
              {table.relationships && table.relationships.length > 0 && (
                <div style={{ marginTop: '1rem' }}>
                  <h5>İlişkiler ({table.relationships.length}):</h5>
                  <ul>
                    {table.relationships.map((rel, relIndex) => (
                      <li key={relIndex}>
                        {table.table}.{rel.key} → {rel.relationTable}.{rel.relationKey}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          </details>
        ))}
      </div>
    );
  };

  return (
    <div className="fieldset">
      <div 
        className="collapsible-header"
        onClick={() => setIsCollapsed(!isCollapsed)}
      >
        <div className="legend">Database Yapısı {isCollapsed ? '▼' : '▲'}</div>
      </div>
      
      {!isCollapsed && (
        <div className="collapsible-content">
          <div className="toolbar">
            <button 
              type="button" 
              className="button"
              onClick={handleInspect}
              disabled={isLoading}
            >
              {isLoading ? 'İnceleniyor...' : 'Database Yapısını İncele'}
            </button>
          </div>
          
          {isVisible && (
            <div style={{ marginTop: '1rem', borderTop: '1px solid #333', paddingTop: '1rem' }}>
              {error ? (
                <div className="error">{error}</div>
              ) : (
                renderDatabaseStructure()
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default DatabaseInspector;