from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import polars as pl
import logging
from datetime import datetime
from pathlib import Path


def _get_timestamp() -> str:
    """Returns the current timestamp as a string."""
    return datetime.now().strftime("%Y%m%d%H%M%S")

class Write(ABC):
    def __init__(self, output_dir: Optional[str] = None):
        self.directory = Path(output_dir or "data")
        self._ensure_directory()

    def _ensure_directory(self) -> None:
        """Ensures that the output directory exists."""
        self.directory.mkdir(parents=True, exist_ok=True)

    def write(self, data: List[Dict[str, Any]]) -> Optional[str]:
        """"Writes data to a file.

        Args:
            data (List[Dict[str, Any]]): The data to write, represented as a list of dictionaries.
        """
        logging.info("Writing data to file.")
        if data is None:
            logging.error("No data provided to write.")
            raise ValueError("Data cannot be None.")
        elif data == []:
            logging.warning("Empty data provided to write. No file will be created.")
            return
        try:
            filename = self.directory / f"output_{_get_timestamp()}.{self._get_extension()}"
            df = pl.DataFrame(data)
            self._do_write(df, filename)
            logging.info("Data successfully written to file.")
            return filename
        except Exception as e:
            logging.error(f"Failed to write file: {e}")
            raise
    @abstractmethod
    def _do_write(self, data: pl.DataFrame, filename: Path) -> None:
        """Helper method to write data to a file. This method can be overridden by subclasses to implement specific writing logic.

        Args:
            filename (Path): The name of the file to write to.
        """
        pass
    @abstractmethod
    def _get_extension(self) -> str:
        """Returns the file extension for the output file."""
        pass

class ParquetWrite(Write):
    def _get_extension(self) -> str:
        return "parquet"

    def _do_write(self, data: pl.DataFrame, filename: Path) -> None:
        data.write_parquet(filename)     
        
class CsvWrite(Write):
    def _get_extension(self) -> str:
        return "csv"

    def _do_write(self, data: pl.DataFrame, filename: Path) -> None:
        data.write_csv(filename)        

class Read(ABC):
    def read(self, filename: Path) -> pl.DataFrame:
        """Reads data from a file.

        Args:
            filename (Path): The name of the file to read from.
        """
        logging.info(f"Reading data from file: {filename}")
        if not filename:
            logging.error("No filename provided to read.")
            raise ValueError("Filename cannot be None.")
        try:
            df = self._do_read(filename)
            logging.info("Data successfully read from file.")
            return df
        except Exception as e:
            logging.error(f"Failed to read file: {e}")
            raise
    @abstractmethod
    def _do_read(self, filename: Path) -> pl.DataFrame:
        """Helper method to read data from a file. This method can be overridden by subclasses to implement specific reading logic.

        Args:
            filename (Path): The name of the file to read from.
        """
        pass

class ParquetRead(Read):
    def _do_read(self, filename: Path) -> pl.DataFrame:
        return pl.read_parquet(filename)

class CsvRead(Read):
    def _do_read(self, filename: Path) -> pl.DataFrame:
        return pl.read_csv(filename)

def main():
    logging.basicConfig(level=logging.INFO)
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ]
    write= ParquetWrite()
    write.write(data)
    read = ParquetRead()
    df = read.read(Path("test.parquet"))
    print(df)

if __name__ == "__main__":
    main()
