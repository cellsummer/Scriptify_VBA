"""
DBF Table Reader/Writer for Polars DataFrames

This module provides functionality to read and write DBF (dBase) files
to/from Polars DataFrames, following the DBF file format specifications.

Supported field types:
- C: Character (string)
- N: Numeric (integer/float)
- L: Logical (boolean)
- D: Date
- F: Float
- M: Memo (treated as string)
"""

import struct
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple
import polars as pl


class DBFFieldDescriptor:
    """Represents a DBF field descriptor."""
    
    def __init__(self, name: str, field_type: str, length: int, decimal_count: int = 0):
        self.name = name[:10].ljust(11, '\x00')  # Field name (11 bytes, null-terminated)
        self.field_type = field_type.upper()
        self.length = length
        self.decimal_count = decimal_count
        
    def to_bytes(self) -> bytes:
        """Convert field descriptor to bytes."""
        return struct.pack(
            '<11sBBBB16s',
            self.name.encode('ascii'),
            ord(self.field_type),
            self.length,
            self.decimal_count,
            0,  # Reserved
            b'\x00' * 16  # Reserved
        )
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'DBFFieldDescriptor':
        """Create field descriptor from bytes."""
        unpacked = struct.unpack('<11sBBBB16s', data)
        name = unpacked[0].rstrip(b'\x00').decode('ascii')
        field_type = chr(unpacked[1])
        length = unpacked[2]
        decimal_count = unpacked[3]
        return cls(name, field_type, length, decimal_count)


class DBFReader:
    """DBF file reader that converts to Polars DataFrame."""
    
    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)
        self.header = None
        self.fields = []
        self.records = []
    
    def read(self) -> pl.DataFrame:
        """Read DBF file and return Polars DataFrame."""
        with open(self.file_path, 'rb') as f:
            # Read header
            self._read_header(f)
            
            # Read field descriptors
            self._read_field_descriptors(f)
            
            # Read records
            self._read_records(f)
        
        return self._create_dataframe()
    
    def _read_header(self, f):
        """Read DBF header (32 bytes)."""
        header_data = f.read(32)
        if len(header_data) < 32:
            raise ValueError("Invalid DBF file: header too short")
        
        self.header = struct.unpack('<BBBBLHHHHHHHHHHH', header_data)
        
        # Extract key information
        self.version = self.header[0]
        self.last_update = datetime.date(
            1900 + self.header[1],  # Year
            self.header[2],         # Month
            self.header[3]          # Day
        )
        self.num_records = self.header[4]
        self.header_length = self.header[5]
        self.record_length = self.header[6]
    
    def _read_field_descriptors(self, f):
        """Read field descriptors."""
        self.fields = []
        
        # Calculate number of field descriptors
        num_fields = (self.header_length - 33) // 32  # 33 = header + terminator
        
        for _ in range(num_fields):
            field_data = f.read(32)
            if len(field_data) < 32:
                break
            
            field = DBFFieldDescriptor.from_bytes(field_data)
            self.fields.append(field)
        
        # Read field descriptor terminator (0x0D)
        terminator = f.read(1)
        if terminator != b'\x0D':
            raise ValueError("Invalid DBF file: missing field descriptor terminator")
    
    def _read_records(self, f):
        """Read all data records."""
        self.records = []
        
        for _ in range(self.num_records):
            record_data = f.read(self.record_length)
            if len(record_data) < self.record_length:
                break
            
            # Check deletion flag (first byte)
            deletion_flag = record_data[0:1]
            if deletion_flag == b'*':
                continue  # Skip deleted records
            
            # Parse record data
            record = self._parse_record(record_data[1:])  # Skip deletion flag
            self.records.append(record)
    
    def _parse_record(self, record_data: bytes) -> Dict[str, Any]:
        """Parse a single record."""
        record = {}
        offset = 0
        
        for field in self.fields:
            field_name = field.name.rstrip('\x00')
            field_data = record_data[offset:offset + field.length]
            offset += field.length
            
            # Convert field data based on type
            value = self._convert_field_value(field_data, field.field_type, field.decimal_count)
            record[field_name] = value
        
        return record
    
    def _convert_field_value(self, data: bytes, field_type: str, decimal_count: int) -> Any:
        """Convert field data to appropriate Python type."""
        try:
            data_str = data.decode('ascii').strip()
        except UnicodeDecodeError:
            data_str = data.decode('latin-1').strip()
        
        if not data_str:
            return None
        
        if field_type == 'C':  # Character
            return data_str
        elif field_type == 'N':  # Numeric
            if decimal_count > 0:
                return float(data_str) if data_str else None
            else:
                return int(data_str) if data_str else None
        elif field_type == 'F':  # Float
            return float(data_str) if data_str else None
        elif field_type == 'L':  # Logical
            return data_str.upper() in ('T', 'Y', '1') if data_str else None
        elif field_type == 'D':  # Date
            if len(data_str) == 8:
                try:
                    year = int(data_str[:4])
                    month = int(data_str[4:6])
                    day = int(data_str[6:8])
                    return datetime.date(year, month, day)
                except ValueError:
                    return None
            return None
        elif field_type == 'M':  # Memo
            return data_str
        else:
            return data_str  # Default to string
    
    def _create_dataframe(self) -> pl.DataFrame:
        """Create Polars DataFrame from records."""
        if not self.records:
            return pl.DataFrame()
        
        # Convert records to column-oriented format
        columns = {}
        for field in self.fields:
            field_name = field.name.rstrip('\x00')
            columns[field_name] = [record.get(field_name) for record in self.records]
        
        return pl.DataFrame(columns)


class DBFWriter:
    """DBF file writer that converts from Polars DataFrame."""
    
    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)
    
    def write(self, df: pl.DataFrame, field_specs: Optional[Dict[str, Dict[str, Any]]] = None):
        """Write Polars DataFrame to DBF file."""
        if df.is_empty():
            raise ValueError("Cannot write empty DataFrame to DBF")
        
        # Infer field specifications if not provided
        if field_specs is None:
            field_specs = self._infer_field_specs(df)
        
        # Create field descriptors
        fields = self._create_field_descriptors(df.columns, field_specs)
        
        # Calculate record length
        record_length = 1 + sum(field.length for field in fields)  # +1 for deletion flag
        
        # Write DBF file
        with open(self.file_path, 'wb') as f:
            # Write header
            self._write_header(f, len(df), len(fields), record_length)
            
            # Write field descriptors
            self._write_field_descriptors(f, fields)
            
            # Write records
            self._write_records(f, df, fields)
    
    def _infer_field_specs(self, df: pl.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Infer field specifications from DataFrame."""
        field_specs = {}
        
        for col_name in df.columns:
            dtype = df[col_name].dtype
            
            if dtype == pl.String:
                # Calculate max string length
                max_len = df[col_name].str.len_chars().max() or 10
                field_specs[col_name] = {'type': 'C', 'length': min(max_len, 254)}
            elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
                field_specs[col_name] = {'type': 'N', 'length': 18, 'decimal': 0}
            elif dtype in (pl.Float32, pl.Float64):
                field_specs[col_name] = {'type': 'F', 'length': 20, 'decimal': 6}
            elif dtype == pl.Boolean:
                field_specs[col_name] = {'type': 'L', 'length': 1}
            elif dtype == pl.Date:
                field_specs[col_name] = {'type': 'D', 'length': 8}
            else:
                # Default to character
                field_specs[col_name] = {'type': 'C', 'length': 50}
        
        return field_specs
    
    def _create_field_descriptors(self, columns: List[str], field_specs: Dict[str, Dict[str, Any]]) -> List[DBFFieldDescriptor]:
        """Create field descriptors from column names and specifications."""
        fields = []
        
        for col_name in columns:
            spec = field_specs[col_name]
            field = DBFFieldDescriptor(
                name=col_name,
                field_type=spec['type'],
                length=spec['length'],
                decimal_count=spec.get('decimal', 0)
            )
            fields.append(field)
        
        return fields
    
    def _write_header(self, f, num_records: int, num_fields: int, record_length: int):
        """Write DBF header."""
        today = datetime.date.today()
        header_length = 32 + (num_fields * 32) + 1  # Header + field descriptors + terminator
        
        header = struct.pack(
            '<BBBBLHHHHHHHHHHH',
            0x03,                    # Version
            today.year - 1900,       # Last update year
            today.month,             # Last update month
            today.day,               # Last update day
            num_records,             # Number of records
            header_length,           # Header length
            record_length,           # Record length
            0, 0, 0, 0, 0, 0, 0, 0, 0  # Reserved fields
        )
        
        f.write(header)
    
    def _write_field_descriptors(self, f, fields: List[DBFFieldDescriptor]):
        """Write field descriptors."""
        for field in fields:
            f.write(field.to_bytes())
        
        # Write terminator
        f.write(b'\x0D')
    
    def _write_records(self, f, df: pl.DataFrame, fields: List[DBFFieldDescriptor]):
        """Write data records."""
        for row in df.iter_rows(named=True):
            # Write deletion flag (space = not deleted)
            f.write(b' ')
            
            # Write field data
            for field in fields:
                field_name = field.name.rstrip('\x00')
                value = row.get(field_name)
                field_data = self._format_field_value(value, field)
                f.write(field_data)
        
        # Write end-of-file marker
        f.write(b'\x1A')
    
    def _format_field_value(self, value: Any, field: DBFFieldDescriptor) -> bytes:
        """Format field value according to field type."""
        if value is None:
            return b' ' * field.length
        
        field_type = field.field_type
        length = field.length
        
        if field_type == 'C':  # Character
            str_val = str(value)[:length]
            return str_val.ljust(length).encode('ascii', errors='replace')
        elif field_type == 'N':  # Numeric
            if field.decimal_count > 0:
                format_str = f'{{:>{length}.{field.decimal_count}f}}'
            else:
                format_str = f'{{:>{length}d}}'
            try:
                formatted = format_str.format(float(value) if field.decimal_count > 0 else int(value))
                return formatted.encode('ascii')
            except (ValueError, OverflowError):
                return b' ' * length
        elif field_type == 'F':  # Float
            try:
                formatted = f'{float(value):>{length}.{field.decimal_count}f}'
                return formatted.encode('ascii')
            except (ValueError, OverflowError):
                return b' ' * length
        elif field_type == 'L':  # Logical
            return b'T' if value else b'F'
        elif field_type == 'D':  # Date
            if isinstance(value, datetime.date):
                date_str = value.strftime('%Y%m%d')
                return date_str.encode('ascii')
            return b' ' * length
        else:
            # Default to character
            str_val = str(value)[:length]
            return str_val.ljust(length).encode('ascii', errors='replace')


# Convenience functions
def read_dbf(file_path: Union[str, Path]) -> pl.DataFrame:
    """Read DBF file and return Polars DataFrame."""
    reader = DBFReader(file_path)
    return reader.read()


def write_dbf(df: pl.DataFrame, file_path: Union[str, Path], 
             field_specs: Optional[Dict[str, Dict[str, Any]]] = None):
    """Write Polars DataFrame to DBF file."""
    writer = DBFWriter(file_path)
    writer.write(df, field_specs)


# Example usage
if __name__ == "__main__":
    # Create sample data
    sample_data = pl.DataFrame({
        'ID': [1, 2, 3, 4, 5],
        'NAME': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'AGE': [25, 30, 35, 28, 32],
        'SALARY': [50000.0, 75000.0, 80000.0, 65000.0, 70000.0],
        'ACTIVE': [True, True, False, True, True],
        'HIRE_DATE': [
            datetime.date(2020, 1, 15),
            datetime.date(2019, 3, 22),
            datetime.date(2018, 7, 10),
            datetime.date(2021, 5, 8),
            datetime.date(2020, 11, 30)
        ]
    })
    
    # Write to DBF
    print("Writing sample data to DBF file...")
    write_dbf(sample_data, 'sample.dbf')
    
    # Read back from DBF
    print("Reading data back from DBF file...")
    df_read = read_dbf('sample.dbf')
    print(df_read)
    
    # Custom field specifications example
    custom_specs = {
        'ID': {'type': 'N', 'length': 10, 'decimal': 0},
        'NAME': {'type': 'C', 'length': 30},
        'AGE': {'type': 'N', 'length': 3, 'decimal': 0},
        'SALARY': {'type': 'F', 'length': 12, 'decimal': 2},
        'ACTIVE': {'type': 'L', 'length': 1},
        'HIRE_DATE': {'type': 'D', 'length': 8}
    }
    
    print("\nWriting with custom field specifications...")
    write_dbf(sample_data, 'sample_custom.dbf', custom_specs)
    df_custom = read_dbf('sample_custom.dbf')
    print(df_custom)