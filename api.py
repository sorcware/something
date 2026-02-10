from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from main import FileConverter
from pathlib import Path
import polars as pl
from pydantic import BaseModel
from typing import Annotated

class QueryRequest(BaseModel):
    file_path: str
    sql: str

app = FastAPI()

READERS = {
        ".parquet": pl.read_parquet,
        ".csv": pl.read_csv,
    }

@app.post("/uploadfile/")
async def upload_file(output_format: Annotated[str, Form()],file: UploadFile = File(...)):
    try:
        # your existing code
        temp_path = Path("uploads") / file.filename
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        with temp_path.open("wb") as f:
            f.write(await file.read())
        converter = FileConverter(input_path=temp_path, output_extension=output_format)
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