import { useState, useCallback } from 'react'
import './App.css'
import DatabaseConnection from './components/DatabaseConnection'
import DatabaseInspector from './components/DatabaseInspector'
import QueryInterface from './components/QueryInterface'
import OutputDatabase from './components/OutputDatabase'
import QueryResults from './components/QueryResults'
import ComparisonTool from './components/ComparisonTool'

function App() {
  const [dbConfig, setDbConfig] = useState({
    dbType: 'postgres',
    connectionString: ''
  })
  
  const [outputConfig, setOutputConfig] = useState({
    enabled: false,
    dbType: 'postgres',
    connectionString: '',
    tableName: 'result_table'
  })
  
  const [inspectionData, setInspectionData] = useState(null)
  const [queryResults, setQueryResults] = useState(null)
  const [queryStatus, setQueryStatus] = useState('')

  const handleDbConfigChange = useCallback((newConfig) => {
    setDbConfig(newConfig)
  }, [])

  const handleOutputConfigChange = useCallback((newConfig) => {
    setOutputConfig(newConfig)
  }, [])

  const handleInspectionData = useCallback((data) => {
    setInspectionData(data)
  }, [])

  const handleQueryResults = useCallback((results) => {
    setQueryResults(results)
  }, [])

  const handleQueryStatus = useCallback((status) => {
    setQueryStatus(status)
  }, [])

  return (
    <div className="app">
      <header className="app-header">
        <h1>Dynamic Query Console</h1>
        <p>Veritabanı sorgularını çalıştırın, sonuçları karşılaştırın ve başka bir veritabanına kaydedin.</p>
      </header>

            <main className="app-main">
        <DatabaseConnection 
          config={dbConfig}
          onChange={handleDbConfigChange}
        />
        
        <DatabaseInspector 
          config={dbConfig}
          data={inspectionData}
          onInspect={handleInspectionData}
        />
        
        <QueryInterface 
          dbConfig={dbConfig}
          outputConfig={outputConfig}
          onResults={handleQueryResults}
          onStatus={handleQueryStatus}
        />
        
        <OutputDatabase 
          config={outputConfig}
          onChange={handleOutputConfigChange}
        />
        
        <QueryResults 
          results={queryResults}
          status={queryStatus}
        />
        {dbConfig.dbType == "postgres" || dbConfig.dbType == "mysql" || dbConfig.dbType == "mssql" || dbConfig.dbType == "oracle" ? (
          <ComparisonTool 
            dbConfig={dbConfig}
          />
        ) : null}
      </main>
    </div>
  )
}

export default App
