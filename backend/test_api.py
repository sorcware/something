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
    data2 = [
        {"name": "Charlie", "age": 35, "sex": "M"},
        {"name": "David", "age": 40, "sex": "M"}
    ]
    parquet_path = tmp_path / "test.parquet"
    pl.DataFrame(data).write_parquet(parquet_path)
    parquet_path2 = tmp_path / "test2.parquet"
    pl.DataFrame(data2).write_parquet(parquet_path2)
    tables_dir = Path(tmp_path)
    tables_dir.mkdir(exist_ok=True)
    destination = tables_dir / "test_table.parquet"
    parquet_path.rename(destination)
    destination2 = tables_dir / "test_table2.parquet"
    parquet_path2.rename(destination2)
    client = TestClient(app)
    response = client.post("/query", json={"file_store": str(tmp_path), "sql": "SELECT * FROM test_table as t cross join test_table2 as t2"})
    assert response.status_code == 200
    result = response.json()["result"]
    print(result)
    assert len(result) == 4
    assert result[0]["name"] == "Alice"
    assert result[0]["age"] == 30
    assert result[0]["name:t2"] == "Charlie"
    assert result[0]["age:t2"] == 35
    assert result[0]["sex"] == "M"
    destination.unlink()
    destination2.unlink()

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
    response = client.post("/query", json={"sql": "This is not SQL"})
    assert response.status_code == 400
    print(response.json())
    assert response.json()["detail"] == "sql parser error: Expected: an SQL statement, found: This at Line: 1, Column: 1"
    destination.unlink()

def test_query_table_no_tables():
    client = TestClient(app)
    response = client.post("/query", json={"sql": "SELECT * FROM table_test"})
    assert response.status_code == 400
    print(response.json())
    assert response.json()["detail"] == "relation 'table_test' was not found"

def test_query_nested_table(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    parquet_path = tmp_path / "test.parquet"
    pl.DataFrame(data).write_parquet(parquet_path)
    nested_dir = tmp_path / "nested/subdir"
    nested_dir.mkdir(parents=True, exist_ok=True)
    destination = nested_dir / "test_table.parquet"
    parquet_path.rename(destination)
    client = TestClient(app)
    response = client.post("/query", json={"file_store": str(tmp_path), "sql": "SELECT * FROM nested__subdir__test_table"})
    assert response.status_code == 200
    result = response.json()["result"]