# File Converter Learning Project

A Python project demonstrating object-oriented programming patterns and data file format conversion using Polars. I'm not trying to do anything fancy here, just learn.

## About This Project

This project was created as a learning exercise to practice and demonstrate various Python programming concepts and patterns. It implements a flexible file converter that can transform data between CSV and Parquet formats. All of the code was written by humans with a little bit of auto complete for good measure. Any AI was used as a teacher only.

## What This Demonstrates

### 1. **Abstract Base Classes (ABC)**
- Uses Python's `abc` module to define abstract interfaces
- `Read` and `Write` base classes define contracts for file operations
- Forces subclasses to implement specific methods (`_do_read`, `_do_write`, `_get_extension`)

### 2. **Inheritance and Polymorphism**
- Concrete implementations: `CsvRead`, `ParquetRead`, `CsvWrite`, `ParquetWrite`
- Each subclass provides format-specific logic while sharing common functionality
- Demonstrates the Open/Closed Principle (open for extension, closed for modification)

### 3. **Factory Pattern**
- `FileConverter.FORMATS` dictionary maps file extensions to appropriate reader/writer classes
- Dynamic class instantiation based on file type
- Makes it easy to add new formats without modifying core logic

### 4. **Type Hints and Type Safety**
- Uses `typing` module for better code documentation and IDE support
- `TypedDict` for structured dictionaries (`ConvertFile`, `ConvertResult`)
- Type hints for method parameters and return values

### 5. **Error Handling and Logging**
- Comprehensive logging throughout the application
- Validates inputs (file existence, format support, empty data)
- Graceful error handling with informative messages

### 6. **Data Processing with Polars**
- Uses the Polars library for efficient DataFrame operations
- Demonstrates reading, writing, and transforming tabular data
- Includes SQL query capabilities on DataFrames

### 7. **Batch Processing**
- `batch_convert()` function handles multiple file conversions
- Returns detailed results for each operation (success/failure)
- Continues processing even if individual conversions fail

### 8. **File System Operations**
- Uses `pathlib.Path` for cross-platform file path handling
- Automatic directory creation
- Timestamped output files to prevent overwrites
## Project Structure
```
.
├── main.py           # Core implementation
├── test_main.py      # Unit tests
├── pyproject.toml    # Project configuration
└── README.md         # This file
```

## Learning Objectives Achieved
* Understanding of abstract base classes and interfaces  
* Implementing inheritance hierarchies  
* Using factory patterns for flexible object creation  
* Writing type-safe Python code with type hints  
* Implementing comprehensive error handling and logging  
* Working with modern data processing libraries (Polars)  
* Creating reusable, extensible code architecture  
* Path manipulation and file I/O operations  