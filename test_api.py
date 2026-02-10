from fastapi.testclient import TestClient
from api import app

def test_upload_file_and_query():
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    client = TestClient(app)
    with open("test.csv", "rb") as f:
        response = client.post("/uploadfile", data={"output_format": ".parquet"}, files={"file": ("test.csv", f, "text/csv")})
    assert response.status_code == 200
    file_path = response.json()["file_path"]
    sql_query = "SELECT * FROM self"
    response = client.post("/query", json={"file_path": file_path, "sql": sql_query})
    assert response.status_code == 200
    result = response.json()["result"]
    assert result == data 