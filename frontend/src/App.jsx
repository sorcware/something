import { useState, useEffect} from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';

function App() {
  return (
    <BrowserRouter>
      <div>
        <h1>File Query Tool</h1>
        <nav>
          <Link to="/convert">Convert Files</Link> | 
          <Link to="/savetable">Save to Table</Link> | 
          <Link to="/query">Query Tables</Link>
        </nav>
        <Routes>
          <Route path="/convert" element={<ConvertSection />} />
          <Route path="/savetable" element={<SaveTableTab />} />
          <Route path="/query" element={<QuerySection />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

function logEvent(event, metadata) 
  {
  const timestamp = new Date().toISOString();
  const eventData = { event, timestamp, metadata };
  
  fetch('http://localhost:8000/event/', 
    { method: 'POST', headers: { 'Content-Type': 'application/json' },
     body: JSON.stringify(eventData) }); }


function ConvertSection() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [convertedFilePath, setConvertedFilePath] = useState("");
  const [format, setFormat] = useState(".parquet");
  const [isUploading, setIsUploading] = useState(false);
  const [error, setError] = useState("");
  const handleUpload = async () => {
    logEvent("upload_clicked", {file_name: selectedFile ? selectedFile.name : null, output_format: format});
    if (!selectedFile) {
      setError("Please select a file to upload.");
      return;
    }
    setError("");
    setIsUploading(true);
    setConvertedFilePath("");
    try {
      const formData = new FormData();
      formData.append("file", selectedFile);
      formData.append("output_format", format);
      const response = await fetch('http://localhost:8000/uploadfile/', {
        method: 'POST',
        body: formData
      });
      const data = await response.json();
      if (response.ok) {
        setConvertedFilePath(data.file_path);
        logEvent("upload_success", {file_name: selectedFile ? selectedFile.name : null, output_format: format, output_file_path: data.file_path});
      } else {
        setError(data.detail || "Upload failed");
        logEvent("upload_failed", {file_name: selectedFile ? selectedFile.name : null, output_format: format, error: data.detail || "Upload failed"});
      }
    } catch (err) {
      setError("An error occurred during upload.");
      logEvent("upload_failed", {file_name: selectedFile ? selectedFile.name : null, output_format: format, error: err.message || "An error occurred during upload."});
    }
    setIsUploading(false);
  };
  const handleDownload = () => {
    if (!convertedFilePath) return;
    logEvent("download_clicked", {file_path: convertedFilePath});
    window.open(`http://localhost:8000/download/${convertedFilePath}`, '_blank');
  };
  return (
    <div>
      <h2>Upload File</h2>
      <input 
        type="file" 
        onChange={(e) => setSelectedFile(e.target.files[0])}
      />
      <select 
        value={format} 
        onChange={(e) => setFormat(e.target.value)}
      >
        <option value=".parquet">Parquet</option>
        <option value=".csv">CSV</option>
      </select>
      <button onClick={handleUpload}>Upload</button>
      {isUploading && <p>Uploading...</p>}
      {convertedFilePath && (
        <p style={{color: 'green'}}>✓ Uploaded: {convertedFilePath}</p>
      )}
      {error && (
        <p style={{color: 'red'}}>✗ Error: {error}</p>
      )}
      <button onClick={handleDownload}>Download</button>
    </div>
  ); 
}
function SaveTableTab() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [tableName, setTableName] = useState("");
  const [writeMode, setWriteMode] = useState("append");
  const [isUploading, setIsUploading] = useState(false);
  const [writeSuccess, setWriteSuccess] = useState("");
  const [error, setError] = useState("");
  const [tables, setTables] = useState([]);
  
  useEffect(() => {
    fetch('http://localhost:8000/tables/')
      .then(res => res.json())
      .then(data => setTables(data.tables))
      .catch(err => console.error(err));
  }, []);

  const handleUpload = async () => {
    logEvent("upload_clicked", {file_name: selectedFile ? selectedFile.name : null, table_name: tableName, write_mode: writeMode});
    if (!selectedFile) {
      setError("Please select a file to upload.");
      return;
    }
    if (!tableName.trim()) {
      setError("Please enter a table name.");
      return;
    }
    setError("");
    setIsUploading(true);
    setWriteSuccess("");

    try {
      const formData = new FormData();
      formData.append("file", selectedFile);
      formData.append("table_name", tableName);
      formData.append("write_mode", writeMode);
      const response = await fetch('http://localhost:8000/savetable/', {
        method: 'POST',
        body: formData
      });
      const data = await response.json();
      if (response.ok) {
        setWriteSuccess(data.destination);
        logEvent("upload_success", {file_name: selectedFile ? selectedFile.name : null, table_name: tableName, write_mode: writeMode, destination: data.destination});
      } else {
        setError(data.detail || "Upload failed");
        logEvent("upload_failed", {file_name: selectedFile ? selectedFile.name : null, table_name: tableName, write_mode: writeMode, error: data.detail || "Write failed"});
      }
    } catch (err) {
      setError("An error occurred during upload.");
      logEvent("upload_failed", {file_name: selectedFile ? selectedFile.name : null, table_name: tableName, write_mode: writeMode, error: err.message || "An error occurred during table write."});
    }
    setIsUploading(false);
  };
  return (
    <div>
      <h2>Write Table</h2>
      <input 
        type="file" 
        onChange={(e) => setSelectedFile(e.target.files[0])}
      />
      <datalist id="tables">
        {tables.map(table => <option key={table} value={table} />)}
      </datalist>
      <input 
        placeholder="Select or type a name"
        value={tableName}
        onChange={(e) => setTableName(e.target.value)}
        list="tables"
      />
      <select 
        value={writeMode} 
        onChange={(e) => setWriteMode(e.target.value)}
      >
        <option value="append">Append</option>
        <option value="overwrite">Overwrite</option>
      </select>
      <button onClick={handleUpload}>Upload</button>
      {isUploading && <p>Uploading...</p>}
      {writeSuccess && (
        <p style={{color: 'green'}}>✓ Uploaded: {writeSuccess}</p>
      )}
      {error && (
        <p style={{color: 'red'}}>✗ Error: {error}</p>
      )}
    </div>
  );
}

function QuerySection() {
  const [tableName, setTableName] = useState("");
  const [query, setQuery] = useState("");
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [isRunning, setIsRunning] = useState(false);
  const [tables, setTables] = useState([]);
  
  useEffect(() => {
    fetch('http://localhost:8000/tables/')
      .then(res => res.json())
      .then(data => setTables(data.tables))
      .catch(err => console.error(err));
  }, []);

  const handleRunQuery = async () => {
    logEvent("query_clicked", {table_name: tableName, sql: query});
    if (!query.trim()) {
      setError("Please enter a SQL query.");
      return;
    }
    setError("");
    setIsRunning(true);
    try {
      const response = await fetch('http://localhost:8000/query/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_name: tableName, sql: query })
      });
      const data = await response.json();
      if (response.ok) {
        setResult(data.result);
        logEvent("query_success", {table_name: tableName, sql: query, result_count: data.result.length});
      } else {
        setError(data.error || "An error occurred while running the query.");
        logEvent("query_failed", {table_name: tableName, sql: query, error: data.error || "An error occurred while running the query."});
      }
    } catch (err) {
      setError("An error occurred while running the query.");
      logEvent("query_failed", {table_name: tableName, sql: query, error: err.message || "An error occurred while running the query."});
    } finally {
      setIsRunning(false);
    }
  };
  return (
    <div>
      <h2>Query File</h2>
      <select 
        value={tableName} 
        onChange={(e) => setTableName(e.target.value)}
      >
        <option value="">Select a table</option>
        {tables.map(table => <option key={table} value={table}>{table}</option>)}
      </select>
      <p>Querying</p>
      <textarea
        placeholder="SELECT * FROM self"
        rows={5}
        cols={50}
        value={query}
        onChange={(e) => setQuery(e.target.value)}
      />
      <br />
      <button onClick={handleRunQuery} disabled={isRunning}>
        {isRunning ? "Running..." : "Run Query"}
      </button>
      {error && <p style={{ color: "red" }}>✗ Error: {error}</p>}
      {result && (
        <div>
          <h3>Result:</h3>
          <ResultsTable data={result} />
        </div>
      )}
    </div>
  );
}

function ResultsTable({ data }) {
  if (!data || data.length === 0) {
    return <p>No results</p>;
  }
  
  const columns = Object.keys(data[0]);
  
  return (
    <table border="1" style={{ marginTop: '20px', borderCollapse: 'collapse' }}>
      <thead>
        <tr>
          {columns.map(col => <th key={col} style={{ padding: '8px' }}>{col}</th>)}
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => (
          <tr key={i}>
            {columns.map(col => <td key={col} style={{ padding: '8px' }}>{row[col]}</td>)}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
export default App;