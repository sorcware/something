from fastapi.testclient import TestClient
from api import app
import pytest

def test_upload_file_and_query(tmp_path):
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
        response = client.post("/uploadfile", data={"output_format": ".parquet", "output_dir": str(tmp_path)}, files={"file": ("test.csv", f, "text/csv")})
        print(response.json())
    assert response.status_code == 200
    file_path = response.json()["file_path"]
    sql_query = "SELECT * FROM self"
    response = client.post("/query", json={"file_path": file_path, "sql": sql_query})
    assert response.status_code == 200
    result = response.json()["result"]
    assert result == data 