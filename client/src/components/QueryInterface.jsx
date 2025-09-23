import { useState } from 'react';
import { postJson } from '../utils/api';
import { extractRows, extractSql, extractWrittenStatus } from '../utils/dataUtils';

const QueryInterface = ({ dbConfig, outputConfig, onResults, onStatus }) => {
  const [currentMode, setCurrentMode] = useState('dsl');
  const [dslQuery, setDslQuery] = useState('');
  const [sqlQuery, setSqlQuery] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isCollapsed, setIsCollapsed] = useState(false);

  const handleModeChange = (mode) => {
    setCurrentMode(mode);
  };

  const buildPayload = (query) => {
    const payload = {
      dbType: dbConfig.dbType,
      connectionString: dbConfig.connectionString,
      query: query
    };
    
    if (outputConfig.enabled) {
      payload.writeToOutputDb = true;
      payload.outputDbType = outputConfig.dbType;
      payload.outputConnectionString = outputConfig.connectionString;
      payload.outputTableName = outputConfig.tableName;
    }
    
    return payload;
  };

  const handleRunQuery = async () => {
    const queryText = currentMode === 'dsl' ? dslQuery.trim() : sqlQuery.trim();
    const endpoint = currentMode === 'dsl' ? '/api/query' : '/api/query/sql';
    
    if (!queryText) {
      onStatus({ type: 'error', message: 'Boş sorgu' });
      return;
    }
    
    if (!dbConfig.dbType || !dbConfig.connectionString) {
      onStatus({ type: 'error', message: 'Database yapılandırması eksik' });
      return;
    }
    
    setIsLoading(true);
    onStatus({ type: 'loading', message: 'Çalışıyor...' });
    
    try {
      const payload = buildPayload(queryText);
      const response = await postJson(endpoint, payload);
      
      const sql = extractSql(response);
      const rows = extractRows(response);
      const written = extractWrittenStatus(response);
      
      onResults({
        sql,
        rows,
        written,
        mode: currentMode.toUpperCase()
      });
      
      onStatus({ 
        type: 'success', 
        message: `${currentMode.toUpperCase()} sorgu başarılı`,
        written 
      });
      
    } catch (error) {
      onStatus({ type: 'error', message: 'Sorgu hatası' });
      onResults({ error: error.message || error });
    } finally {
      setIsLoading(false);
    }
  };

  const handleClearQuery = () => {
    setDslQuery('');
    setSqlQuery('');
    onResults(null);
    onStatus('');
  };

  return (
    <div className="fieldset">
      <div 
        className="collapsible-header"
        onClick={() => setIsCollapsed(!isCollapsed)}
      >
        <div className="legend">Query Mode {isCollapsed ? '▼' : '▲'}</div>
      </div>
      
      {!isCollapsed && (
        <div className="collapsible-content">
          <div className="mode-selector">
            <button 
              type="button" 
              className={`mode-btn ${currentMode === 'dsl' ? 'active' : ''}`}
              onClick={() => handleModeChange('dsl')}
            >
              DSL Query
            </button>
            <button 
              type="button" 
              className={`mode-btn ${currentMode === 'sql' ? 'active' : ''}`}
              onClick={() => handleModeChange('sql')}
            >
              Raw SQL
            </button>
          </div>
          
          <div className={`query-section ${currentMode === 'dsl' ? 'active' : ''}`}>
            <label className="label" htmlFor="dsl">Custom DSL Sorgu</label>
            <textarea 
              id="dsl"
              className="textarea"
              value={dslQuery}
              onChange={(e) => setDslQuery(e.target.value)}
              placeholder="FETCH(id, name) FILTER(age > 18 AND status = 'A') FROM(users) INCLUDE(orders)"
              rows="4"
            />
            <small className="hint">
              Örnekler: FETCH(id, name) FROM users | FILTER(age &gt; 18) | INCLUDE(orders INNER) | GROUPBY(status) HAVING(COUNT(*) &gt; 5)
            </small>
          </div>
          
          <div className={`query-section ${currentMode === 'sql' ? 'active' : ''}`}>
            <label className="label" htmlFor="sql">Raw SQL Sorgu</label>
            <textarea 
              id="sql"
              className="textarea"
              value={sqlQuery}
              onChange={(e) => setSqlQuery(e.target.value)}
              placeholder="SELECT id, name FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE age > 18 AND status = 'A';"
              rows="4"
            />
            <small className="hint">
              Ham SQL sorgunuzu yazın. Çoklu statement desteklenmez.
            </small>
          </div>

          <div className="toolbar toolbar-end">
            <button 
              type="button" 
              className="button"
              onClick={handleRunQuery}
              disabled={isLoading}
            >
              {isLoading ? 'Çalışıyor...' : 'Sorguyu Çalıştır'}
            </button>
            <button 
              type="button" 
              className="button secondary"
              onClick={handleClearQuery}
            >
              Temizle
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default QueryInterface;