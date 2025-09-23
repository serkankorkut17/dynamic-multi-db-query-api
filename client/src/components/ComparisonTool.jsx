import { useState } from 'react';
import { postJson } from '../utils/api';
import { 
  extractRows, 
  extractSql, 
  formatData,
  canonical,
  canonicalByValues,
  shallowSummary,
  diffArrays
} from '../utils/dataUtils';

const ComparisonTool = ({ dbConfig }) => {
  const [dslQuery, setDslQuery] = useState('');
  const [sqlQuery, setSqlQuery] = useState('');
  const [dslResult, setDslResult] = useState(null);
  const [sqlResult, setSqlResult] = useState(null);
  const [dslData, setDslData] = useState(undefined);
  const [sqlData, setSqlData] = useState(undefined);
  const [compareResult, setCompareResult] = useState('');
  const [loadingStates, setLoadingStates] = useState({ dsl: false, sql: false });
  const [isCollapsed, setIsCollapsed] = useState(false);

  const buildPayload = (query) => ({
    dbType: dbConfig.dbType,
    connectionString: dbConfig.connectionString,
    query: query
  });

  const runQuery = async (type) => {
    const query = type === 'dsl' ? dslQuery.trim() : sqlQuery.trim();
    const endpoint = type === 'dsl' ? '/api/query' : '/api/query/sql';
    
    if (!query) {
      const result = 'Boş sorgu.';
      if (type === 'dsl') {
        setDslResult(result);
        setDslData(undefined);
      } else {
        setSqlResult(result);
        setSqlData(undefined);
      }
      return;
    }

    if (!dbConfig.dbType || !dbConfig.connectionString) {
      const result = 'Database yapılandırması eksik.';
      if (type === 'dsl') {
        setDslResult(result);
        setDslData(undefined);
      } else {
        setSqlResult(result);
        setSqlData(undefined);
      }
      return;
    }

    setLoadingStates(prev => ({ ...prev, [type]: true }));

    try {
      const payload = buildPayload(query);
      const response = await postJson(endpoint, payload);
      
      const sql = extractSql(response);
      const rows = extractRows(response);
      
      let resultText;
      if (type === 'dsl' && sql !== undefined) {
        resultText = `Generated SQL:\n${sql}\n\nResult:\n${formatData(rows)}`;
        setDslData(rows);
      } else {
        resultText = formatData(rows);
        if (type === 'dsl') {
          setDslData(rows);
        } else {
          setSqlData(rows);
        }
      }
      
      if (type === 'dsl') {
        setDslResult(resultText);
      } else {
        setSqlResult(resultText);
      }
      
    } catch (error) {
      const errorText = formatData(error);
      if (type === 'dsl') {
        setDslResult(errorText);
        setDslData(undefined);
      } else {
        setSqlResult(errorText);
        setSqlData(undefined);
      }
    } finally {
      setLoadingStates(prev => ({ ...prev, [type]: false }));
    }
  };

  const compareResults = () => {
    if (dslData === undefined || sqlData === undefined) {
      setCompareResult('Karşılaştırma için her iki sorguyu da önce çalıştır.');
      return;
    }

    try {
      let equal;
      if (Array.isArray(dslData) && Array.isArray(sqlData)) {
        const canonDsl = canonicalByValues(dslData);
        const canonSql = canonicalByValues(sqlData);
        equal = canonDsl === canonSql;
      } else {
        const canonDsl = canonical(dslData);
        const canonSql = canonical(sqlData);
        equal = canonDsl === canonSql;
      }

      let lines = [];
      lines.push(equal ? 'SONUÇ: AYNI ✅' : 'SONUÇ: FARKLI ❌');

      if (!equal) {
        if (Array.isArray(dslData) && Array.isArray(sqlData)) {
          lines.push('Özet DSL: ' + shallowSummary(dslData));
          lines.push('Özet SQL: ' + shallowSummary(sqlData));
          lines.push(...diffArrays(dslData, sqlData));
        }
      }

      setCompareResult(lines.join('\n'));
    } catch (err) {
      setCompareResult('Karşılaştırma hatası: ' + err.message);
    }
  };

  const clearAll = () => {
    setDslQuery('');
    setSqlQuery('');
    setDslResult('(sonuç yok)');
    setSqlResult('(sonuç yok)');
    setDslData(undefined);
    setSqlData(undefined);
    setCompareResult('(karşılaştırma sonucu)');
  };

  const clearDsl = () => {
    setDslQuery('');
    setDslResult('(temizlendi)');
    setDslData(undefined);
  };

  const clearSql = () => {
    setSqlQuery('');
    setSqlResult('(temizlendi)');
    setSqlData(undefined);
  };

  return (
    <div className="fieldset">
      <div 
        className="collapsible-header"
        onClick={() => setIsCollapsed(!isCollapsed)}
      >
        <div className="legend">Karşılaştırma Aracı {isCollapsed ? '▼' : '▲'}</div>
      </div>
      
      {!isCollapsed && (
        <div className="collapsible-content">
          <p>İki farklı sorgu sonucunu karşılaştırmak için bu bölümü kullanabilirsiniz.</p>
          
          <div className="row">
            <div className="col">
              <div className="fieldset">
                <div className="legend">DSL Query</div>
                <textarea 
                  className="textarea"
                  value={dslQuery}
                  onChange={(e) => setDslQuery(e.target.value)}
                  placeholder="FETCH(id, name) FROM users"
                  rows="3"
                />
                <div className="toolbar toolbar-end">
                  <button 
                    type="button" 
                    className="button"
                    onClick={() => runQuery('dsl')}
                    disabled={loadingStates.dsl}
                  >
                    {loadingStates.dsl ? 'Çalışıyor...' : 'Çalıştır'}
                  </button>
                  <button 
                    type="button" 
                    className="button secondary"
                    onClick={clearDsl}
                  >
                    Temizle
                  </button>
                </div>
                <pre className="comparison-result-pre">
                  {dslResult || '(sonuç yok)'}
                </pre>
              </div>
            </div>
            
            <div className="col">
              <div className="fieldset">
                <div className="legend">SQL Query</div>
                <textarea 
                  className="textarea"
                  value={sqlQuery}
                  onChange={(e) => setSqlQuery(e.target.value)}
                  placeholder="SELECT id, name FROM users"
                  rows="3"
                />
                <div className="toolbar toolbar-end">
                  <button 
                    type="button" 
                    className="button"
                    onClick={() => runQuery('sql')}
                    disabled={loadingStates.sql}
                  >
                    {loadingStates.sql ? 'Çalışıyor...' : 'Çalıştır'}
                  </button>
                  <button 
                    type="button" 
                    className="button secondary"
                    onClick={clearSql}
                  >
                    Temizle
                  </button>
                </div>
                <pre className="comparison-result-pre">
                  {sqlResult || '(sonuç yok)'}
                </pre>
              </div>
            </div>
          </div>
          
          <div className="toolbar">
            <button 
              type="button" 
              className="button"
              onClick={compareResults}
            >
              Sonuçları Karşılaştır
            </button>
            <button 
              type="button" 
              className="button secondary"
              onClick={clearAll}
            >
              Hepsini Temizle
            </button>
          </div>
          
          <pre className="comparison-main-result">
            {compareResult || '(karşılaştırma sonucu)'}
          </pre>
        </div>
      )}
    </div>
  );
};

export default ComparisonTool;