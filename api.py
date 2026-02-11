from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from main import FileConverter
from pathlib import Path
import polars as pl
from pydantic import BaseModel
from typing import Annotated

class UploadRequest(BaseModel):
    output_format: str
    output_dir: str | None = None

class QueryRequest(BaseModel):
    file_path: str
    sql: str

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5175", "http://127.0.0.1:5175"],
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
        file_extension = Path(request.file_path).suffix
        reader_function = READERS.get(file_extension)
        if reader_function is None:
            raise ValueError(f"Unsupported file format: {file_extension}")
        df = reader_function(request.file_path).lazy().sql(request.sql).collect()
        return {"result": df.to_dicts()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))