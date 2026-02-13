from fastapi.testclient import TestClient
from api import app
import pytest
import polars as pl
from pathlib import Path

def test_convert_file(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    csv_path = tmp_path / "test.csv"
    with open(csv_path, "w") as f:
        f.write("name,age\n")
        for item in data:
            f.write(f"{item['name']},{item['age']}\n")

    client = TestClient(app)
    with open(csv_path, "rb") as f:
        response = client.post("/convertfile", data={"output_format": ".parquet", "output_dir": str(tmp_path)}, files={"file": ("test.csv", f, "text/csv")})
    assert response.status_code == 200
    file_path = response.json()["file_path"]
    assert (tmp_path / file_path).exists()

def test_convert_file_invalid_format(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    csv_path = tmp_path / "test.csv"
    with open(csv_path, "w") as f:
        f.write("name,age\n")
        for item in data:
            f.write(f"{item['name']},{item['age']}\n")

    client = TestClient(app)
    with open(csv_path, "rb") as f:
        response = client.post("/convertfile", data={"output_format": ".invalid", "output_dir": str(tmp_path)}, files={"file": ("test.csv", f, "text/csv")})
    assert response.status_code == 400
    print(response.json())
    assert response.json()["detail"] == "Unsupported output file format: .invalid"

def test_convert_file_no_file(tmp_path):
    client = TestClient(app)
    response = client.post("/convertfile", data={"output_format": ".parquet", "output_dir": str(tmp_path)})
    assert response.status_code == 422

def test_save_to_table(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    csv_path = tmp_path / "test.csv"
    with open(csv_path, "w") as f:
        f.write("name,age\n")
        for item in data:
            f.write(f"{item['name']},{item['age']}\n")

    client = TestClient(app)
    with open(csv_path, "rb") as f:
        response = client.post("/savetable", data={"table_name": "test_table", "write_mode": "overwrite"}, files={"file": ("test.csv", f, "text/csv")})
    assert response.status_code == 200
    destination = response.json()["destination"]
    assert (Path(destination)).exists()
    Path(destination).unlink()

def test_query_table(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    parquet_path = tmp_path / "test.parquet"
    pl.DataFrame(data).write_parquet(parquet_path)
    tables_dir = Path("tables")
    tables_dir.mkdir(exist_ok=True)
    destination = tables_dir / "test_table.parquet"
    parquet_path.rename(destination)
    client = TestClient(app)
    response = client.post("/query", json={"table_name": "test_table", "sql": "SELECT * FROM self"})
    assert response.status_code == 200
    result = response.json()["result"]
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[0]["age"] == 30
    destination.unlink()

def test_query_table_invalid_sql(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    parquet_path = tmp_path / "test.parquet"
    pl.DataFrame(data).write_parquet(parquet_path)
    tables_dir = Path("tables")
    tables_dir.mkdir(exist_ok=True)
    destination = tables_dir / "test_table.parquet"
    parquet_path.rename(destination)
    client = TestClient(app)
    response = client.post("/query", json={"table_name": "test_table", "sql": "This is not SQL"})
    assert response.status_code == 500
    print(response.json())
    assert response.json()["detail"] == "sql parser error: Expected: an SQL statement, found: This at Line: 1, Column: 1"
    destination.unlink()