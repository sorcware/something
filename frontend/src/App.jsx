import { useState } from 'react';

function App() {
  const [uploadedFilePath, setUploadedFilePath] = useState("");
  return (
    <div>
      <h1>File Query Tool</h1>
      <UploadSection 
        onUploadSuccess={setUploadedFilePath} 
        uploadedFilePath={uploadedFilePath}
      />
      {uploadedFilePath && <QuerySection filePath={uploadedFilePath} />}
    </div>
  );
}

function UploadSection({ onUploadSuccess, uploadedFilePath }) {
  const [selectedFile, setSelectedFile] = useState(null);
  const [format, setFormat] = useState(".parquet");
  const [isUploading, setIsUploading] = useState(false);
  const [error, setError] = useState("");
  
  const handleUpload = async () => {
    if (!selectedFile) {
      setError("Please select a file to upload.");
      return;
    }
    if (selectedFile) {
      setError("");
      setIsUploading(true);
      try {
        const formData = new FormData();
        formData.append("file", selectedFile);
        formData.append("output_format", format);

        fetch('http://localhost:8000/uploadfile/', {
          method: 'POST',
          body: formData
        })
        .then(response => response.json())
        .then(data => {
          onUploadSuccess(data.file_path);
        })
        .catch(error => {
          setError("An error occurred during upload.");
        })
        .finally(() => {
          setIsUploading(false);
        });
      } catch (err) {
        setError("An error occurred during upload.");
        setIsUploading(false);
      }
    }
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
      {uploadedFilePath && (
        <p style={{color: 'green'}}>✓ Uploaded: {uploadedFilePath}</p>
      )}
      {error && (
        <p style={{color: 'red'}}>✗ Error: {error}</p>
      )}
    </div>
  );
}

function QuerySection({ filePath }) {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [isRunning, setIsRunning] = useState(false);

  const handleRunQuery = async () => {
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
        body: JSON.stringify({ file_path: filePath, sql: query })
      });
      const data = await response.json();
      if (response.ok) {
        setResult(data.result);
      } else {
        setError(data.error || "An error occurred while running the query.");
      }
    } catch (err) {
      setError("An error occurred while running the query.");
    } finally {
      setIsRunning(false);
    }
  };
  return (
    <div>
      <h2>Query File</h2>
      <p>Querying: {filePath}</p>
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