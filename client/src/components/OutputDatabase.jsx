import { useState, useEffect, useCallback } from 'react';
import { getDefaultConnectionString } from '../utils/api';

const OutputDatabase = ({ config, onChange }) => {
  const [userEditedConnection, setUserEditedConnection] = useState(false);
  const [isCollapsed, setIsCollapsed] = useState(false);

  const handleEnabledChange = useCallback((e) => {
    onChange({ ...config, enabled: e.target.checked });
  }, [config, onChange]);

  const handleDbTypeChange = useCallback((e) => {
    const newType = e.target.value;
    const newConfig = { ...config, dbType: newType };
    
    // Auto-fill connection string if user hasn't manually edited it
    if (!userEditedConnection) {
      newConfig.connectionString = getDefaultConnectionString(newType);
    }
    
    onChange(newConfig);
  }, [config, userEditedConnection, onChange]);

  const handleConnectionChange = useCallback((e) => {
    setUserEditedConnection(true);
    onChange({ ...config, connectionString: e.target.value });
  }, [config, onChange]);

  const handleTableNameChange = useCallback((e) => {
    onChange({ ...config, tableName: e.target.value });
  }, [config, onChange]);

  // Initialize with default connection string if empty
  useEffect(() => {
    if (config.enabled && !config.connectionString && !userEditedConnection) {
      onChange({
        ...config,
        connectionString: getDefaultConnectionString(config.dbType)
      });
    }
  }, [config, userEditedConnection, onChange]);

  return (
    <div className="fieldset">
      <div 
        className="collapsible-header"
        onClick={() => setIsCollapsed(!isCollapsed)}
      >
        <div className="legend">Output Database {isCollapsed ? '▼' : '▲'}</div>
      </div>
      
      {!isCollapsed && (
        <div className="collapsible-content">
          <div className="checkbox-field">
            <input 
              type="checkbox" 
              id="writeToOutput"
              checked={config.enabled}
              onChange={handleEnabledChange}
            />
            <label htmlFor="writeToOutput">
              Sonuçları başka bir veritabanına kaydet
            </label>
          </div>

          {config.enabled && (
            <div style={{ marginTop: '1rem' }}>
              <div className="row">
                <div className="col">
                  <label className="label" htmlFor="outputDbType">Output DB Type</label>
                  <select 
                    id="outputDbType" 
                    className="select"
                    value={config.dbType}
                    onChange={handleDbTypeChange}
                  >
                    <option value="postgres">PostgreSQL</option>
                    <option value="mysql">MySQL</option>
                    <option value="mssql">SQL Server</option>
                    <option value="oracle">Oracle</option>
                  </select>
                </div>
                <div className="col">
                  <label className="label" htmlFor="outputTableName">Table Name</label>
                  <input 
                    type="text" 
                    id="outputTableName"
                    className="input"
                    value={config.tableName}
                    onChange={handleTableNameChange}
                    placeholder="result_table"
                  />
                </div>
              </div>
              
              <label className="label" htmlFor="outputConn">Output Connection String</label>
              <textarea 
                id="outputConn"
                className="textarea"
                value={config.connectionString}
                onChange={handleConnectionChange}
                placeholder="Output database connection string"
                rows="3"
              />
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default OutputDatabase;