import { useState, useEffect, useCallback } from 'react';
import { getDefaultConnectionString } from '../utils/api';

const DatabaseConnection = ({ config, onChange }) => {
  const [userEditedConnection, setUserEditedConnection] = useState(false);
  const [isCollapsed, setIsCollapsed] = useState(false);

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

  // Initialize with default connection string if empty
  useEffect(() => {
    if (!config.connectionString && !userEditedConnection) {
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
        <div className="legend">Input Database {isCollapsed ? '▼' : '▲'}</div>
      </div>
      
      {!isCollapsed && (
        <div className="collapsible-content">
          <div className="row">
            <div className="col">
              <label className="label" htmlFor="dbType">DB Type</label>
              <select 
                id="dbType" 
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
          </div>
          
          <label className="label" htmlFor="conn">Connection String</label>
          <textarea 
            id="conn"
            className="textarea"
            value={config.connectionString}
            onChange={handleConnectionChange}
            placeholder="Database connection string"
            rows="3"
          />
        </div>
      )}
    </div>
  );
};

export default DatabaseConnection;