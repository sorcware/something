from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from main import FileConverter, TableWrite, get_table_tree, _flatten_tables
from pathlib import Path
import polars as pl
from pydantic import BaseModel
from typing import Annotated
from fastapi.responses import FileResponse
import json

class UploadRequest(BaseModel):
    output_format: str
    output_dir: str | None = None

class QueryRequest(BaseModel):
    file_store: str | None = "tables"
    sql: str

class EventRequest(BaseModel):
    event: str
    timestamp: str
    metadata: dict

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

READERS = {
        ".parquet": pl.read_parquet,
        ".csv": pl.read_csv,
    }

@app.post("/convertfile/")
async def upload_file(file: UploadFile = File(...),
    output_format: str = Form(...),
    output_dir: str | None = Form(None)):
    try:
        temp_path = Path("uploads") / file.filename
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        with temp_path.open("wb") as f:
            f.write(await file.read())
        converter = FileConverter(input_path=temp_path, output_extension=output_format, output_dir=output_dir)
        file_path = converter.convert()
        temp_path.unlink()
        return {"file_path": str(file_path)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def query_file(request: QueryRequest):
    try:
        all_tables = json.loads(get_table_tree(request.file_store))["tables"]
        if not all_tables:
            raise ValueError("No tables available to query.")
        dfs = {}
        print(f"Available tables: {all_tables}")
        flattened_tables = _flatten_tables(all_tables)
        print(f"Flattened tables: {flattened_tables}")
        for file in flattened_tables:
            file_path = Path(request.file_store) / file["path"]
            print(f"Checking for table file: {file_path}")
            if not file_path.exists():
                raise ValueError(f"Table file not found: {file_path}")
            dfs[file["df_name"]] = pl.scan_parquet(str(file_path))
            print(f"loaded table: {file['df_name']} from file: {file_path}")
        ctx = pl.SQLContext().register_many(dfs)
        result = ctx.execute(request.sql).collect()
        return {"result": result.to_dicts()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/event")
async def log_event(request: EventRequest):
    with open("events/events.jsonl", "a") as f:
        print(f"Logging event: {request.event} at {request.timestamp} with metadata: {request.metadata}")
        f.write(f"{request.json()}\n")
        return Response(status_code=204)

@app.post("/savetable")
async def save_table(file: UploadFile = File(...), table_name: str = Form(...), write_mode: str = Form(...)):
    try:
        temp_path = Path("uploads") / file.filename
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        with temp_path.open("wb") as f:
            f.write(await file.read())
        file_extension = temp_path.suffix
        if file_extension not in READERS:
            raise ValueError(f"Unsupported file format: {file_extension}")
        reader_function = READERS.get(file_extension)
        df = reader_function(temp_path)
        writer = TableWrite(table_name, write_mode)
        destination = writer.write(df)
        temp_path.unlink()
        return {"destination": str(destination)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables")
async def list_tables():
     return json.loads(get_table_tree())

@app.get("/download/{file_path:path}")
async def download_file(file_path: str):
    try:
        full_path = Path(file_path).resolve()
        project_root = Path.cwd()

        if not str(full_path).startswith(str(project_root / "data")):
            raise HTTPException(status_code=403, detail="Access denied")
        
        if not full_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        
        return FileResponse(
            path=full_path,
            filename=full_path.name,
            media_type='application/octet-stream'
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))