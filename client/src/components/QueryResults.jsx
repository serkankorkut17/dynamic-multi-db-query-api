import { useState } from 'react';
import { formatData } from '../utils/dataUtils';

const QueryResults = ({ results, status }) => {
  const [isSqlCollapsed, setIsSqlCollapsed] = useState(true);
  const [isCollapsed, setIsCollapsed] = useState(false);
  const renderStatus = () => {
    if (!status) return null;

    if (typeof status === 'string') {
      return <div>{status}</div>;
    }

    let statusClass = 'status-badge ';
    switch (status.type) {
      case 'success':
        statusClass += 'status-success';
        break;
      case 'error':
        statusClass += 'status-error';
        break;
      case 'loading':
        statusClass += 'status-warning';
        break;
      case 'info':
      default:
        statusClass += 'status-info';
        break;
    }

    return (
      <div style={{ marginBottom: '1rem' }}>
        <span className={statusClass}>
          ✓ {status.message}
        </span>
        {status.written && (
          <span className="status-badge status-success">
            ✓ Output DB&apos;ye kaydedildi
          </span>
        )}
      </div>
    );
  };

  const renderDataTable = (data) => {
    if (!Array.isArray(data) || data.length === 0) {
      return <p>Veri bulunamadı</p>;
    }

    const headers = Object.keys(data[0]);

    return (
      <table className="data-table">
        <thead>
          <tr>
            {headers.map(header => (
              <th key={header}>{header}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, index) => (
            <tr key={index}>
              {headers.map(header => {
                const val = row[header];
                const displayVal = val === null ? <em>null</em> : 
                  typeof val === 'object' ? JSON.stringify(val) : String(val);
                return <td key={header}>{displayVal}</td>;
              })}
            </tr>
          ))}
        </tbody>
      </table>
    );
  };

  const renderResults = () => {
    if (!results) {
      return <div>(henüz sorgu çalıştırılmadı)</div>;
    }

    if (results.error) {
      return (
        <div className="error">
          <pre>{formatData(results.error)}</pre>
        </div>
      );
    }

    return (
      <div>
        {results.sql && (
          <div className="generated-sql-section">
            <div 
              className="generated-sql-header"
              onClick={() => setIsSqlCollapsed(!isSqlCollapsed)}
            >
              <h4>Generated SQL {isSqlCollapsed ? '▼' : '▲'}</h4>
            </div>
            {!isSqlCollapsed && (
              <div className="generated-sql-content">
                <pre className="generated-sql-pre">
                  {results.sql}
                </pre>
              </div>
            )}
          </div>
        )}
        
        <h4>
          Sonuçlar ({Array.isArray(results.rows) ? results.rows.length : 'N/A'} satır):
        </h4>
        
        {Array.isArray(results.rows) && results.rows.length > 0 ? (
          renderDataTable(results.rows)
        ) : (
          <p>Veri bulunamadı veya boş sonuç</p>
        )}
      </div>
    );
  };

  return (
    <div className="fieldset">
      <div 
        className="collapsible-header"
        onClick={() => setIsCollapsed(!isCollapsed)}
      >
        <div className="legend">Query Sonuçları {isCollapsed ? '▼' : '▲'}</div>
      </div>
      
      {!isCollapsed && (
        <div className="collapsible-content">
          {renderStatus()}
          {renderResults()}
        </div>
      )}
    </div>
  );
};

export default QueryResults;