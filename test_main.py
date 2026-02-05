import pytest
import polars as pl

from main import ParquetWrite, CsvWrite, ParquetRead, CsvRead

def test_parquet_write(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    writer = ParquetWrite(output_dir=tmp_path)
    filename = writer.write(data)
    assert pl.read_parquet(filename).equals(pl.DataFrame(data))

def test_parquet_write_none_data(tmp_path):
    data = None
    writer = ParquetWrite(output_dir=tmp_path)
    with pytest.raises(ValueError):
        writer.write(data)

def test_parquet_write_empty_data(tmp_path):
    data = []
    writer = ParquetWrite(output_dir=tmp_path)
    assert writer.write(data) is None

def test_csv_write(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    writer = CsvWrite(output_dir=tmp_path)
    filename = writer.write(data)
    assert pl.read_csv(filename).equals(pl.DataFrame(data))

def test_csv_write_none_data(tmp_path):
    data = None
    writer = CsvWrite(output_dir=tmp_path)
    with pytest.raises(ValueError):
        writer.write(data)

def test_csv_write_empty_data(tmp_path):
    data = []
    writer = CsvWrite(output_dir=tmp_path)
    assert writer.write(data) is None

def test_parquet_read(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    filename = tmp_path / "test.parquet"
    pl.DataFrame(data).write_parquet(filename)
    reader = ParquetRead()
    df = reader.read(filename)
    assert df.equals(pl.DataFrame(data))

def test_parquet_read_no_filename():
    reader = ParquetRead()
    with pytest.raises(ValueError):
        reader.read(None)

def test_csv_read(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    filename = tmp_path / "test.csv"
    pl.DataFrame(data).write_csv(filename)
    reader = CsvRead()
    df = reader.read(filename)
    assert df.equals(pl.DataFrame(data))

def test_csv_read_no_filename():
    reader = CsvRead()
    with pytest.raises(ValueError):
        reader.read(None)