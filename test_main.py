import pytest
import polars as pl

from main import ParquetWrite, CsvWrite, ParquetRead, CsvRead, FileConverter, batch_convert

from pathlib import Path

def test_parquet_write(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    writer = ParquetWrite(input_filename="output", output_dir=tmp_path)
    filename = writer.write(data)
    assert pl.read_parquet(filename).equals(pl.DataFrame(data))

def test_parquet_write_none_data(tmp_path):
    data = None
    writer = ParquetWrite(input_filename="output", output_dir=tmp_path)
    with pytest.raises(ValueError):
        writer.write(data)

def test_parquet_write_empty_data(tmp_path):
    data = []
    writer = ParquetWrite(input_filename="output", output_dir=tmp_path)
    assert writer.write(data) is None

def test_csv_write(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    writer = CsvWrite(input_filename="output", output_dir=tmp_path)
    filename = writer.write(data)
    assert pl.read_csv(filename).equals(pl.DataFrame(data))

def test_csv_write_none_data(tmp_path):
    data = None
    writer = CsvWrite(input_filename="output", output_dir=tmp_path)
    with pytest.raises(ValueError):
        writer.write(data)

def test_csv_write_empty_data(tmp_path):
    data = []
    writer = CsvWrite(input_filename="output", output_dir=tmp_path)
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

def test_file_converter_csv_to_parquet(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    csv_filename = tmp_path / "test.csv"
    pl.DataFrame(data).write_csv(csv_filename)
    converter = FileConverter(input_path=csv_filename, output_extension=".parquet", output_dir=tmp_path)
    parquet_filename = converter.convert()
    assert pl.read_parquet(parquet_filename).equals(pl.DataFrame(data))

def test_file_converter_parquet_to_csv(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    parquet_filename = tmp_path / "test.parquet"
    pl.DataFrame(data).write_parquet(parquet_filename)
    converter = FileConverter(input_path=parquet_filename, output_extension=".csv", output_dir=tmp_path)
    csv_filename = converter.convert()
    assert pl.read_csv(csv_filename).equals(pl.DataFrame(data))

def test_file_converter_same_format(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    csv_filename = tmp_path / "test.csv"
    pl.DataFrame(data).write_csv(csv_filename)
    converter = FileConverter(input_path=csv_filename, output_extension=".csv", output_dir=tmp_path)
    with pytest.raises(ValueError):
        converter.convert()

def test_file_converter_input_file_not_exist(tmp_path):
    converter = FileConverter(input_path=tmp_path / "nonexistent.csv", output_extension=".parquet", output_dir=tmp_path)
    with pytest.raises(FileNotFoundError):
        converter.convert()

def test_file_converter_unsupported_input_format(tmp_path):
    converter = FileConverter(input_path=tmp_path / "test.txt", output_extension=".parquet", output_dir=tmp_path)
    with pytest.raises(ValueError):
        converter.convert()

def test_file_converter_unsupported_output_format(tmp_path):
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    csv_filename = tmp_path / "test.csv"
    pl.DataFrame(data).write_csv(csv_filename)
    converter = FileConverter(input_path=csv_filename, output_extension=".txt", output_dir=tmp_path)
    with pytest.raises(ValueError):
        converter.convert()

def test_batch_file_converter(tmp_path):
    data1 = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    data2 = [
        {"name": "Charlie", "age": 35},
        {"name": "David", "age": 40}
    ]
    csv_filename = tmp_path / "test.csv"
    parquet_filename = tmp_path / "test.parquet"
    pl.DataFrame(data1).write_csv(csv_filename)
    pl.DataFrame(data2).write_parquet(parquet_filename)
    
    batch_files = [
        {"input_path": Path(csv_filename), "output_extension": ".parquet", "output_dir": tmp_path / "test"},
        {"input_path": Path(parquet_filename), "output_extension": ".csv", "output_dir": tmp_path / "test"},
    ]
    results = batch_convert(batch_files)
    assert len(results) == 2
    assert results[0]["success"] is True
    assert pl.read_parquet(results[0]["output_path"]).equals(pl.DataFrame(data1))
    assert results[1]["success"] is True
    assert pl.read_csv(results[1]["output_path"]).equals(pl.DataFrame(data2))
    
def test_batch_file_converter_file_does_not_exist(tmp_path):
    data1 = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]
    data2 = [
        {"name": "Charlie", "age": 35},
        {"name": "David", "age": 40}
    ]
    csv_filename = tmp_path / "test.csv"
    csv_filename2 = tmp_path / "test2.csv"
    pl.DataFrame(data1).write_csv(csv_filename)
    pl.DataFrame(data2).write_csv(csv_filename2)
    
    batch_files = [
        {"input_path": Path(csv_filename), "output_extension": ".parquet", "output_dir": tmp_path / "test"},
        {"input_path": Path(tmp_path / "nonexistent.parquet"), "output_extension": ".csv", "output_dir": tmp_path / "test"},
        {"input_path": Path(csv_filename2), "output_extension": ".parquet", "output_dir": tmp_path / "test"},
    ]
    results = batch_convert(batch_files)
    assert len(results) == 3
    assert results[0]["success"] is True
    assert pl.read_parquet(results[0]["output_path"]).equals(pl.DataFrame(data1))
    assert results[1]["success"] is False
    assert results[2]["success"] is True
    assert pl.read_parquet(results[2]["output_path"]).equals(pl.DataFrame(data2))