from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, TypedDict
import polars as pl
import logging
from datetime import datetime
from pathlib import Path


def _get_timestamp() -> str:
    """Returns the current timestamp as a string."""
    return datetime.now().strftime("%Y%m%d%H%M%S")

class ConvertFile(TypedDict):
    input_path: Path
    output_extension: str
    output_dir: Optional[str]

class ConvertResult(TypedDict):
    input_path: Path
    output_path: Optional[Path]
    success: bool
    error_message: Optional[str]

class Write(ABC):
    def __init__(self, input_filename: str, output_dir: Optional[str] = None):
        self.directory = Path(output_dir or "data")
        self.input_filename = input_filename or "output"
        self._ensure_directory()

    def _ensure_directory(self) -> None:
        """Ensures that the output directory exists."""
        self.directory.mkdir(parents=True, exist_ok=True)

    def write(self, data: List[Dict[str, Any]]) -> Optional[Path]:
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
            return None
        try:
            filename = self.directory / f"{self.input_filename}_{_get_timestamp()}.{self._get_extension()}"
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
        if not filename.exists():
            logging.error(f"File not found: {filename}")
            raise FileNotFoundError(f"File not found: {filename}")
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

class FileConverter:
    FORMATS = {
        ".parquet": (ParquetRead, ParquetWrite),
        ".csv": (CsvRead, CsvWrite),
    }

    def __init__(self, input_path: Path, output_extension: str, output_dir: Optional[str] = None):
        self.input_path = input_path
        self.output_extension = output_extension
        self.input_extension = self.input_path.suffix
        self.output_dir = output_dir
        self.input_filename = self.input_path.stem
    
    def _get_read_classes(self, extension: str) -> type[Read]:
        """Retrieves the appropriate reader classes based on the file extension.

        Args:
            extension (str): The file extension.
        Returns:
            type[Read]: The reader classes for the specified file format.
        Raises:
            ValueError: If the file format is not supported.
        """
        return self.FORMATS[extension][0]

    def _get_write_classes(self, extension: str) -> type[Write]:
        """Retrieves the appropriate writer classes based on the file extension.

        Args:
            extension (str): The file extension.
        Returns:
            type[Write]: The writer classes for the specified file format.
        Raises:
            ValueError: If the file format is not supported.
        """
        return self.FORMATS[extension][1]
    
    def _validate_formats(self) -> bool:
        """Validates that the input and output formats are supported and not the same."""
        if self.input_extension not in self.FORMATS:
            logging.error(f"Unsupported input file format: {self.input_extension}")
            raise ValueError(f"Unsupported input file format: {self.input_extension}")
        if self.output_extension not in self.FORMATS:
            logging.error(f"Unsupported output file format: {self.output_extension}")
            raise ValueError(f"Unsupported output file format: {self.output_extension}")
        if self.input_extension == self.output_extension:
            logging.error("Input and output formats are the same. No conversion will be performed.")
            raise ValueError("Input and output formats cannot be the same.")
        return True

    def convert(self) -> Optional[Path]:
        self._validate_formats()
        reader_class = self._get_read_classes(self.input_extension)
        writer_class = self._get_write_classes(self.output_extension)
        reader = reader_class()
        writer = writer_class(output_dir=self.output_dir, input_filename=self.input_filename)
        df = reader.read(self.input_path)
        return writer.write(df.to_dicts())

def batch_convert(files: List[ConvertFile]) -> List[ConvertResult]:
    """Converts a batch of files based on the provided list of file paths and desired output formats.

    Args:
        files (List[ConvertFile]): A list of dictionaries, each containing an 'input_path' and 'output_extension' key.
    Returns:
        List[ConvertResult]: A list of results for each file conversion, including success status and any error messages.
    """
    results: List[ConvertResult] = []
    for file in files:
        try:
            converter = FileConverter(input_path=file["input_path"], output_extension=file["output_extension"], output_dir=file.get("output_dir"))
            output_path = converter.convert()
            results.append({"input_path": file["input_path"], "output_path": output_path, "success": True, "error_message": None})
        except Exception as e:
            logging.error(f"Failed to convert file {file['input_path']}: {e}")
            results.append({"input_path": file["input_path"], "output_path": None, "success": False, "error_message": str(e)})
    return results
        

def main() -> Optional[Path]:
    logging.basicConfig(level=logging.INFO)
    # data = [
    #     {"name": "Alice", "age": 30},
    #     {"name": "Bob", "age": 25},
    # ]
    # write= ParquetWrite()
    # write.write(data)
    # read = ParquetRead()
    # df = read.read(Path("test.parquet"))
    # print(df)
    # converter = FileConverter(input_path=Path("test.parquet"), output_extension=".csv", output_dir="converted")
    # file_name = converter.convert()
    # print(f"File converted and saved as: {file_name}")

    # batch_files = [
    #     {"input_path": Path("test.parquet"), "output_extension": ".csv", "output_dir": "converted"},
    #     {"input_path": Path("doesnotexist.csv"), "output_extension": ".parquet"},
    # ]
    # results = batch_convert(batch_files)
    # print(results)
    reader = CsvRead()
    df = reader.read(Path("test.csv"))
    results = pl.sql("SELECT * from df").collect()
    print(results)

if __name__ == "__main__":
    main()
