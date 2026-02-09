from fastapi import FastAPI, UploadFile, File, HTTPException
from main import FileConverter
from pathlib import Path
import polars as pl


app = FastAPI()

READERS = {
        ".parquet": pl.read_parquet,
        ".csv": pl.read_csv,
    }

@app.post("/uploadfile/")
async def upload_file(Output_format: str, file: UploadFile = File(...)):
    try:
        # your existing code
        temp_path = Path("uploads") / file.filename
        temp_path.parent.mkdir(exist_ok=True)
        with temp_path.open("wb") as f:
            f.write(await file.read())
        converter = FileConverter(input_path=temp_path, output_extension=Output_format)
        file_path = converter.convert()
        temp_path.unlink()
        return {"file_path": str(file_path)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def query_file(file_path: str, sql: str):
    try:
        file_extension = Path(file_path).suffix
        reader_function = READERS.get(file_extension)
        if reader_function is None:
            raise ValueError(f"Unsupported file format: {file_extension}")
        df = reader_function(file_path).lazy().sql(sql).collect()
        return {"result": df.to_dicts()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))