from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from main import FileConverter, TableWrite
from pathlib import Path
import polars as pl
from pydantic import BaseModel
from typing import Annotated

class UploadRequest(BaseModel):
    output_format: str
    output_dir: str | None = None

class QueryRequest(BaseModel):
    table_name: str
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

@app.post("/uploadfile/")
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
        file_path = f"tables/{request.table_name}.parquet"
        reader_function = READERS.get(".parquet")
        df = reader_function(file_path).lazy().sql(request.sql).collect()
        return {"result": df.to_dicts()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/event")
async def log_event(request: EventRequest):
    with open("events/events.jsonl", "a") as f:
        print(f"Logging event: {request.event} at {request.timestamp} with metadata: {request.metadata}")
        f.write(f"{request.json()}\n")

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
    tables_dir = Path("tables")
    if not tables_dir.exists():
        return {"tables": []}
    table_files = list(tables_dir.glob("*.parquet"))
    table_names = [file.stem for file in table_files]
    return {"tables": table_names}

from fastapi.responses import FileResponse

@app.get("/download/{file_path:path}")
async def download_file(file_path: str):
    try:
        full_path = Path(file_path).resolve()
        project_root = Path.cwd()
        
        if not str(full_path).startswith(str(project_root)):
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