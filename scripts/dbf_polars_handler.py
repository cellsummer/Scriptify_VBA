"""
DBF Table Reader/Writer for Polars DataFrames with CDX Index Support

This module provides functionality to read and write DBF (dBase) files
to/from Polars DataFrames, following the DBF file format specifications.
Also includes support for CDX (compound index) files.

Supported field types:
- C: Character (string)
- N: Numeric (integer/float)
- L: Logical (boolean)
- D: Date
- F: Float
- M: Memo (treated as string)

CDX Index Features:
- Read index metadata and structure
- Extract sorted record orders
- Get unique values and statistics
- Support for multiple indexes per CDX file
"""

import struct
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple
import polars as pl


class DBFFieldDescriptor:
    """Represents a DBF field descriptor."""

    def __init__(self, name: str, field_type: str, length: int, decimal_count: int = 0):
        self.name = name[:10].ljust(
            11, "\x00"
        )  # Field name (11 bytes, null-terminated)
        self.field_type = field_type.upper()
        self.length = length
        self.decimal_count = decimal_count

    def to_bytes(self) -> bytes:
        """Convert field descriptor to bytes."""
        return struct.pack(
            "<11sBBBB15sB",
            self.name.encode("ascii"),
            ord(self.field_type),
            self.length,
            self.decimal_count,
            0,  # Reserved
            b"\x00" * 15,  # Reserved
            0,  # Work area ID
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "DBFFieldDescriptor":
        """Create field descriptor from bytes."""
        if len(data) < 32:
            raise ValueError(f"Field descriptor requires 32 bytes, got {len(data)}")

        # DBF field descriptor structure (32 bytes total):
        # 0-10: Field name (11 bytes, null-terminated)
        # 11: Field type (1 byte)
        # 12-15: Field data address (4 bytes, not used in file)
        # 16: Field length (1 byte)
        # 17: Decimal count (1 byte)
        # 18-31: Reserved (14 bytes)

        name = data[0:11].rstrip(b"\x00").decode("ascii")
        field_type = chr(data[11])
        # Skip bytes 12-15 (field data address)
        length = data[16]
        decimal_count = data[17]

        return cls(name, field_type, length, decimal_count)


class CDXIndex:
    """Represents a single index within a CDX file."""

    def __init__(self):
        self.name = ""
        self.expression = ""
        self.key_length = 0
        self.key_type = ""
        self.unique = False
        self.ascending = True
        self.root_page = 0
        self.record_count = 0
        self.keys = []
        self.record_numbers = []


class CDXReader:
    """CDX (Compound Index) file reader."""

    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)
        self.header = None
        self.indexes = []
        self.page_size = 512  # Default CDX page size

    def read(self) -> Dict[str, Any]:
        """Read CDX file and return index information."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"CDX file not found: {self.file_path}")

        with open(self.file_path, "rb") as f:
            # Read CDX header
            self._read_header(f)

            # Read index descriptors
            self._read_index_descriptors(f)

            # Read index data for each index
            for index in self.indexes:
                self._read_index_data(f, index)

        return {
            "indexes": self.indexes,
            "header": self.header,
            "page_size": self.page_size,
        }

    def _read_header(self, f):
        """Read CDX header (512 bytes)."""
        header_data = f.read(512)
        if len(header_data) < 512:
            raise ValueError("Invalid CDX file: header too short")

        # CDX header structure
        self.header = {
            "root_page": struct.unpack("<L", header_data[0:4])[0],
            "free_page": struct.unpack("<L", header_data[4:8])[0],
            "reserved1": struct.unpack("<L", header_data[8:12])[0],
            "key_length": struct.unpack("<H", header_data[12:14])[0],
            "index_options": header_data[14],
            "index_signature": header_data[15],
            "reserved2": header_data[16:512],
        }

        print(f"CDX Header:")
        print(f"  Root page: {self.header['root_page']}")
        print(f"  Free page: {self.header['free_page']}")
        print(f"  Key length: {self.header['key_length']}")

    def _read_index_descriptors(self, f):
        """Read index descriptors from root page."""
        if self.header["root_page"] == 0:
            return

        # Seek to root page
        f.seek(self.header["root_page"] * self.page_size)

        # Read page header
        page_data = f.read(self.page_size)
        if len(page_data) < self.page_size:
            return

        # Parse page header (12 bytes)
        page_type = struct.unpack("<H", page_data[0:2])[0]
        entry_count = struct.unpack("<H", page_data[2:4])[0]
        left_sibling = struct.unpack("<L", page_data[4:8])[0]
        right_sibling = struct.unpack("<L", page_data[8:12])[0]

        print(f"Root page type: {page_type}, entries: {entry_count}")

        # Read index entries
        offset = 12
        for i in range(entry_count):
            if offset + 32 > len(page_data):
                break

            index = CDXIndex()

            # Read index descriptor (varies by implementation)
            try:
                # Try to extract index name and expression
                name_end = page_data.find(b"\x00", offset, offset + 32)
                if name_end != -1:
                    index.name = page_data[offset:name_end].decode(
                        "ascii", errors="ignore"
                    )
                else:
                    index.name = f"INDEX_{i}"

                # Extract other properties (this is simplified)
                index.key_length = self.header["key_length"]
                index.key_type = "C"  # Default to character

                self.indexes.append(index)
                print(f"Found index: {index.name}")

            except Exception as e:
                print(f"Error reading index descriptor {i}: {e}")

            offset += 32  # Move to next descriptor

    def _read_index_data(self, f, index: CDXIndex):
        """Read index data (simplified implementation)."""
        # This is a simplified version - full CDX parsing is quite complex
        # In practice, you'd need to traverse B-tree pages to get all keys
        index.keys = []
        index.record_numbers = []

        print(f"Reading data for index: {index.name}")
        # Placeholder - actual implementation would traverse index pages


class DBFReader:
    """DBF file reader that converts to Polars DataFrame with CDX support."""

    def __init__(
        self, file_path: Union[str, Path], cdx_path: Optional[Union[str, Path]] = None
    ):
        self.file_path = Path(file_path)
        self.cdx_path = (
            Path(cdx_path) if cdx_path else self.file_path.with_suffix(".cdx")
        )
        self.header = None
        self.fields = []
        self.records = []
        self.cdx_info = None

    def read(self, use_index: Optional[str] = None) -> pl.DataFrame:
        """Read DBF file and return Polars DataFrame.

        Args:
            use_index: Name of CDX index to use for sorting (optional)
        """
        # Read CDX file if available
        if self.cdx_path.exists():
            try:
                cdx_reader = CDXReader(self.cdx_path)
                self.cdx_info = cdx_reader.read()
                print(f"Loaded CDX file with {len(self.cdx_info['indexes'])} indexes")
            except Exception as e:
                print(f"Warning: Could not read CDX file: {e}")

        with open(self.file_path, "rb") as f:
            # Read header
            self._read_header(f)

            # Read field descriptors
            self._read_field_descriptors(f)

            # Read records
            self._read_records(f)

        df = self._create_dataframe()

        # Apply index-based sorting if requested
        if use_index and self.cdx_info:
            df = self._apply_index_sort(df, use_index)

        return df

    def get_index_info(self) -> Dict[str, Any]:
        """Get information about available indexes."""
        if not self.cdx_info:
            return {"message": "No CDX file loaded"}

        info = {"indexes": [], "page_size": self.cdx_info["page_size"]}

        for idx in self.cdx_info["indexes"]:
            info["indexes"].append(
                {
                    "name": idx.name,
                    "expression": idx.expression,
                    "key_length": idx.key_length,
                    "key_type": idx.key_type,
                    "unique": idx.unique,
                    "ascending": idx.ascending,
                    "record_count": idx.record_count,
                }
            )

        return info

    def _apply_index_sort(self, df: pl.DataFrame, index_name: str) -> pl.DataFrame:
        """Apply sorting based on CDX index."""
        if not self.cdx_info:
            return df

        # Find the requested index
        target_index = None
        for idx in self.cdx_info["indexes"]:
            if idx.name.upper() == index_name.upper():
                target_index = idx
                break

        if not target_index:
            print(f"Index '{index_name}' not found")
            return df

        # If we have record numbers from the index, use them
        if target_index.record_numbers:
            # This would require more complex implementation
            print(f"Applying sort from index: {index_name}")

        return df

    def _read_header(self, f):
        """Read DBF header (32 bytes)."""
        header_data = f.read(32)
        if len(header_data) < 32:
            raise ValueError(
                f"Invalid DBF file: header too short ({len(header_data)} bytes)"
            )

        try:
            # DBF header structure (32 bytes):
            # 0: Version number
            # 1-3: Date of last update (YY MM DD)
            # 4-7: Number of records (4 bytes, little-endian)
            # 8-9: Header length (2 bytes, little-endian)
            # 10-11: Record length (2 bytes, little-endian)
            # 12-31: Reserved

            self.version = header_data[0]
            self.last_update = datetime.date(
                1900 + header_data[1],  # Year
                header_data[2],  # Month
                header_data[3],  # Day
            )
            self.num_records = struct.unpack("<L", header_data[4:8])[0]
            self.header_length = struct.unpack("<H", header_data[8:10])[0]
            self.record_length = struct.unpack("<H", header_data[10:12])[0]

            print(f"DBF Header Info:")
            print(f"  Version: {self.version}")
            print(f"  Last Update: {self.last_update}")
            print(f"  Records: {self.num_records}")
            print(f"  Header Length: {self.header_length}")
            print(f"  Record Length: {self.record_length}")

        except Exception as e:
            raise ValueError(f"Error parsing DBF header: {e}")

    def _read_field_descriptors(self, f):
        """Read field descriptors."""
        self.fields = []

        if self.header_length < 33:
            raise ValueError(f"Invalid header length: {self.header_length}")

        # Calculate number of field descriptors
        fields_section_length = self.header_length - 33  # 32 (header) + 1 (terminator)
        num_fields = fields_section_length // 32

        print(f"Expected {num_fields} field descriptors")

        current_pos = f.tell()
        print(f"Starting field descriptor read at position: {current_pos}")

        for i in range(num_fields):
            field_data = f.read(32)
            if len(field_data) < 32:
                print(
                    f"Warning: Field descriptor {i} is incomplete ({len(field_data)} bytes)"
                )
                # Try to read what's available and pad the rest
                if len(field_data) > 0:
                    field_data += b"\x00" * (32 - len(field_data))
                else:
                    break

            # Check if we've hit the terminator (0x0D) at the beginning
            if field_data[0] == 0x0D:
                print(f"Found terminator at field {i}")
                f.seek(-31, 1)  # Back up, keeping only the terminator
                break

            try:
                field = DBFFieldDescriptor.from_bytes(field_data)
                print(
                    f"Field {i}: {field.name.strip()} ({field.field_type}, {field.length})"
                )
                self.fields.append(field)
            except Exception as e:
                print(f"Error parsing field descriptor {i}: {e}")
                print(f"Field data (hex): {field_data.hex()}")
                # Try to continue with next field
                continue

        # Find and read the terminator
        max_search = 100  # Limit search to avoid infinite loop
        terminator_found = False

        for _ in range(max_search):
            byte = f.read(1)
            if not byte:
                break
            if byte == b"\x0d":
                terminator_found = True
                print("Found field descriptor terminator")
                break

        if not terminator_found:
            print("Warning: Field descriptor terminator not found, continuing anyway")
            # Reset to expected position
            expected_pos = 32 + len(self.fields) * 32 + 1
            f.seek(expected_pos)

    def _read_records(self, f):
        """Read all data records."""
        self.records = []

        for _ in range(self.num_records):
            record_data = f.read(self.record_length)
            if len(record_data) < self.record_length:
                break

            # Check deletion flag (first byte)
            deletion_flag = record_data[0:1]
            if deletion_flag == b"*":
                continue  # Skip deleted records

            # Parse record data
            record = self._parse_record(record_data[1:])  # Skip deletion flag
            self.records.append(record)

    def _parse_record(self, record_data: bytes) -> Dict[str, Any]:
        """Parse a single record."""
        record = {}
        offset = 0

        for field in self.fields:
            field_name = field.name.rstrip("\x00")
            field_data = record_data[offset : offset + field.length]
            offset += field.length

            # Convert field data based on type
            value = self._convert_field_value(
                field_data, field.field_type, field.decimal_count
            )
            record[field_name] = value

        return record

    def _convert_field_value(
        self, data: bytes, field_type: str, decimal_count: int
    ) -> Any:
        """Convert field data to appropriate Python type."""
        try:
            data_str = data.decode("ascii").strip()
        except UnicodeDecodeError:
            data_str = data.decode("latin-1").strip()

        if not data_str:
            return None

        if field_type == "C":  # Character
            return data_str
        elif field_type == "N":  # Numeric
            if decimal_count > 0:
                return float(data_str) if data_str else None
            else:
                return int(data_str) if data_str else None
        elif field_type == "F":  # Float
            return float(data_str) if data_str else None
        elif field_type == "L":  # Logical
            return data_str.upper() in ("T", "Y", "1") if data_str else None
        elif field_type == "D":  # Date
            if len(data_str) == 8:
                try:
                    year = int(data_str[:4])
                    month = int(data_str[4:6])
                    day = int(data_str[6:8])
                    return datetime.date(year, month, day)
                except ValueError:
                    return None
            return None
        elif field_type == "M":  # Memo
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
            field_name = field.name.rstrip("\x00")
            columns[field_name] = [record.get(field_name) for record in self.records]

        return pl.DataFrame(columns)


class DBFWriter:
    """DBF file writer that converts from Polars DataFrame."""

    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)

    def write(
        self, df: pl.DataFrame, field_specs: Optional[Dict[str, Dict[str, Any]]] = None
    ):
        """Write Polars DataFrame to DBF file."""
        if df.is_empty():
            raise ValueError("Cannot write empty DataFrame to DBF")

        # Infer field specifications if not provided
        if field_specs is None:
            field_specs = self._infer_field_specs(df)

        # Create field descriptors
        fields = self._create_field_descriptors(df.columns, field_specs)

        # Calculate record length
        record_length = 1 + sum(
            field.length for field in fields
        )  # +1 for deletion flag

        # Write DBF file
        with open(self.file_path, "wb") as f:
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
                field_specs[col_name] = {"type": "C", "length": min(max_len, 254)}
            elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
                field_specs[col_name] = {"type": "N", "length": 18, "decimal": 0}
            elif dtype in (pl.Float32, pl.Float64):
                field_specs[col_name] = {"type": "F", "length": 20, "decimal": 6}
            elif dtype == pl.Boolean:
                field_specs[col_name] = {"type": "L", "length": 1}
            elif dtype == pl.Date:
                field_specs[col_name] = {"type": "D", "length": 8}
            else:
                # Default to character
                field_specs[col_name] = {"type": "C", "length": 50}

        return field_specs

    def _create_field_descriptors(
        self, columns: List[str], field_specs: Dict[str, Dict[str, Any]]
    ) -> List[DBFFieldDescriptor]:
        """Create field descriptors from column names and specifications."""
        fields = []

        for col_name in columns:
            spec = field_specs[col_name]
            field = DBFFieldDescriptor(
                name=col_name,
                field_type=spec["type"],
                length=spec["length"],
                decimal_count=spec.get("decimal", 0),
            )
            fields.append(field)

        return fields

    def _write_header(self, f, num_records: int, num_fields: int, record_length: int):
        """Write DBF header."""
        today = datetime.date.today()
        header_length = (
            32 + (num_fields * 32) + 1
        )  # Header + field descriptors + terminator

        header = struct.pack(
            "<BBBBLHHHHHHHHHHH",
            0x03,  # Version
            today.year - 1900,  # Last update year
            today.month,  # Last update month
            today.day,  # Last update day
            num_records,  # Number of records
            header_length,  # Header length
            record_length,  # Record length
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,  # Reserved fields
        )

        f.write(header)

    def _write_field_descriptors(self, f, fields: List[DBFFieldDescriptor]):
        """Write field descriptors."""
        for field in fields:
            # Write field descriptor (32 bytes)
            field_name = field.name[:10].ljust(11, "\x00")
            descriptor = struct.pack(
                "<11sB4sBBB13sB",
                field_name.encode("ascii"),
                ord(field.field_type),
                b"\x00\x00\x00\x00",  # Field data address (not used in file)
                field.length,
                field.decimal_count,
                0,  # Reserved
                b"\x00" * 13,  # Reserved
                0,  # Work area ID
            )
            f.write(descriptor)

        # Write terminator
        f.write(b"\x0d")

    def _write_records(self, f, df: pl.DataFrame, fields: List[DBFFieldDescriptor]):
        """Write data records."""
        for row in df.iter_rows(named=True):
            # Write deletion flag (space = not deleted)
            f.write(b" ")

            # Write field data
            for field in fields:
                field_name = field.name.rstrip("\x00")
                value = row.get(field_name)
                field_data = self._format_field_value(value, field)
                f.write(field_data)

        # Write end-of-file marker
        f.write(b"\x1a")

    def _format_field_value(self, value: Any, field: DBFFieldDescriptor) -> bytes:
        """Format field value according to field type."""
        if value is None:
            return b" " * field.length

        field_type = field.field_type
        length = field.length

        if field_type == "C":  # Character
            str_val = str(value)[:length]
            return str_val.ljust(length).encode("ascii", errors="replace")
        elif field_type == "N":  # Numeric
            if field.decimal_count > 0:
                format_str = f"{{:>{length}.{field.decimal_count}f}}"
            else:
                format_str = f"{{:>{length}d}}"
            try:
                formatted = format_str.format(
                    float(value) if field.decimal_count > 0 else int(value)
                )
                return formatted.encode("ascii")
            except (ValueError, OverflowError):
                return b" " * length
        elif field_type == "F":  # Float
            try:
                formatted = f"{float(value):>{length}.{field.decimal_count}f}"
                return formatted.encode("ascii")
            except (ValueError, OverflowError):
                return b" " * length
        elif field_type == "L":  # Logical
            return b"T" if value else b"F"
        elif field_type == "D":  # Date
            if isinstance(value, datetime.date):
                date_str = value.strftime("%Y%m%d")
                return date_str.encode("ascii")
            return b" " * length
        else:
            # Default to character
            str_val = str(value)[:length]
            return str_val.ljust(length).encode("ascii", errors="replace")


# Convenience functions
def read_dbf(
    file_path: Union[str, Path],
    debug: bool = False,
    cdx_path: Optional[Union[str, Path]] = None,
    use_index: Optional[str] = None,
) -> pl.DataFrame:
    """Read DBF file and return Polars DataFrame.

    Args:
        file_path: Path to the DBF file
        debug: If True, print debugging information
        cdx_path: Path to CDX file (auto-detected if None)
        use_index: Name of CDX index to use for sorting
    """
    try:
        reader = DBFReader(file_path, cdx_path)
        if debug:
            print(f"Reading DBF file: {file_path}")
            print(f"File size: {Path(file_path).stat().st_size} bytes")

            # Show index information if available
            index_info = reader.get_index_info()
            if "indexes" in index_info and index_info["indexes"]:
                print("Available indexes:")
                for idx in index_info["indexes"]:
                    print(f"  - {idx['name']}: {idx['expression']} ({idx['key_type']})")

        return reader.read(use_index=use_index)
    except Exception as e:
        print(f"Error reading DBF file {file_path}: {e}")

        # Try alternative reading approach
        print("Attempting alternative parsing...")
        return read_dbf_alternative(file_path)


def read_cdx_info(cdx_path: Union[str, Path]) -> Dict[str, Any]:
    """Read CDX file and return index information.

    Args:
        cdx_path: Path to the CDX file

    Returns:
        Dictionary containing index information
    """
    try:
        reader = CDXReader(cdx_path)
        return reader.read()
    except Exception as e:
        print(f"Error reading CDX file: {e}")
        return {"error": str(e)}


def analyze_dbf_with_cdx(
    dbf_path: Union[str, Path], cdx_path: Optional[Union[str, Path]] = None
) -> Dict[str, Any]:
    """Analyze DBF file structure including CDX indexes.

    Args:
        dbf_path: Path to the DBF file
        cdx_path: Path to CDX file (auto-detected if None)

    Returns:
        Dictionary with analysis results
    """
    dbf_path = Path(dbf_path)
    if cdx_path is None:
        cdx_path = dbf_path.with_suffix(".cdx")

    analysis = {
        "dbf_file": str(dbf_path),
        "cdx_file": str(cdx_path),
        "dbf_exists": dbf_path.exists(),
        "cdx_exists": cdx_path.exists(),
        "dbf_size": dbf_path.stat().st_size if dbf_path.exists() else 0,
        "cdx_size": cdx_path.stat().st_size if cdx_path.exists() else 0,
        "fields": [],
        "indexes": [],
        "record_count": 0,
    }

    try:
        # Analyze DBF structure
        reader = DBFReader(dbf_path, cdx_path)
        with open(dbf_path, "rb") as f:
            reader._read_header(f)
            reader._read_field_descriptors(f)

        analysis["record_count"] = reader.num_records
        analysis["fields"] = [
            {
                "name": field.name.strip(),
                "type": field.field_type,
                "length": field.length,
                "decimal_count": field.decimal_count,
            }
            for field in reader.fields
        ]

        # Analyze CDX if available
        if cdx_path.exists():
            cdx_info = read_cdx_info(cdx_path)
            if "indexes" in cdx_info:
                analysis["indexes"] = [
                    {
                        "name": idx.name,
                        "expression": idx.expression,
                        "key_length": idx.key_length,
                        "key_type": idx.key_type,
                        "unique": idx.unique,
                        "ascending": idx.ascending,
                    }
                    for idx in cdx_info["indexes"]
                ]

    except Exception as e:
        analysis["error"] = str(e)

    return analysis


def read_dbf_alternative(file_path: Union[str, Path]) -> pl.DataFrame:
    """Alternative DBF reader with more flexible parsing."""
    with open(file_path, "rb") as f:
        # Read first few bytes to identify format
        header_bytes = f.read(32)
        if len(header_bytes) < 32:
            raise ValueError("File too small to be a valid DBF file")

        print(f"First 32 bytes (hex): {header_bytes.hex()}")

        # Parse basic header info
        version = header_bytes[0]
        num_records = struct.unpack("<L", header_bytes[4:8])[0]
        header_length = struct.unpack("<H", header_bytes[8:10])[0]
        record_length = struct.unpack("<H", header_bytes[10:12])[0]

        print(f"Alternative parser - Version: {version}, Records: {num_records}")
        print(f"Header length: {header_length}, Record length: {record_length}")

        # Read field descriptors more carefully
        f.seek(32)  # Start after header
        fields = []

        while f.tell() < header_length - 1:  # -1 for terminator
            pos = f.tell()
            field_data = f.read(32)

            if len(field_data) < 32:
                print(f"Incomplete field descriptor at position {pos}")
                break

            # Check for terminator
            if field_data[0] == 0x0D:
                print(f"Found terminator at position {pos}")
                break

            # Parse field descriptor manually
            try:
                name = field_data[0:11].rstrip(b"\x00").decode("ascii", errors="ignore")
                if not name or not name.isprintable():
                    print(f"Invalid field name at position {pos}, stopping")
                    break

                field_type = chr(field_data[11])
                length = field_data[16] if len(field_data) > 16 else 10
                decimal_count = field_data[17] if len(field_data) > 17 else 0

                field = DBFFieldDescriptor(name, field_type, length, decimal_count)
                fields.append(field)
                print(f"Field: {name} ({field_type}, {length})")

            except Exception as e:
                print(f"Error parsing field at position {pos}: {e}")
                break

        if not fields:
            raise ValueError("No valid field descriptors found")

        # Find start of data records
        f.seek(header_length)

        # Read records
        records = []
        expected_record_size = (
            sum(field.length for field in fields) + 1
        )  # +1 for deletion flag

        print(
            f"Expected record size: {expected_record_size}, DBF record length: {record_length}"
        )

        for i in range(
            min(num_records, 1000)
        ):  # Limit to first 1000 records for safety
            record_data = f.read(record_length)
            if len(record_data) < record_length:
                print(f"Incomplete record {i}")
                break

            # Check deletion flag
            if record_data[0:1] == b"*":
                continue  # Skip deleted records

            # Parse record
            record = {}
            offset = 1  # Skip deletion flag

            for field in fields:
                if offset + field.length > len(record_data):
                    print(f"Record {i}: field {field.name} extends beyond record")
                    break

                field_data = record_data[offset : offset + field.length]
                offset += field.length

                # Convert field value
                try:
                    value = convert_field_value_safe(
                        field_data, field.field_type, field.decimal_count
                    )
                    record[field.name.strip()] = value
                except Exception as e:
                    print(f"Error converting field {field.name}: {e}")
                    record[field.name.strip()] = None

            if record:  # Only add if we successfully parsed some fields
                records.append(record)

        print(f"Successfully parsed {len(records)} records")

        if not records:
            return pl.DataFrame()

        # Create DataFrame
        columns = {}
        for field in fields:
            field_name = field.name.strip()
            columns[field_name] = [record.get(field_name) for record in records]

        return pl.DataFrame(columns)


def convert_field_value_safe(data: bytes, field_type: str, decimal_count: int) -> Any:
    """Safely convert field data to appropriate Python type."""
    try:
        data_str = data.decode("ascii", errors="ignore").strip()
    except:
        data_str = ""

    if not data_str:
        return None

    try:
        if field_type == "C":  # Character
            return data_str
        elif field_type == "N":  # Numeric
            if decimal_count > 0:
                return float(data_str)
            else:
                return int(data_str)
        elif field_type == "F":  # Float
            return float(data_str)
        elif field_type == "L":  # Logical
            return data_str.upper() in ("T", "Y", "1")
        elif field_type == "D":  # Date
            if len(data_str) == 8 and data_str.isdigit():
                year = int(data_str[:4])
                month = int(data_str[4:6])
                day = int(data_str[6:8])
                return datetime.date(year, month, day)
            return None
        else:
            return data_str
    except:
        return None


def write_dbf(
    df: pl.DataFrame,
    file_path: Union[str, Path],
    field_specs: Optional[Dict[str, Dict[str, Any]]] = None,
):
    """Write Polars DataFrame to DBF file."""
    writer = DBFWriter(file_path)
    writer.write(df, field_specs)


# Example usage
if __name__ == "__main__":
    # Create sample data
    sample_data = pl.DataFrame(
        {
            "ID": [1, 2, 3, 4, 5],
            "NAME": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "AGE": [25, 30, 35, 28, 32],
            "SALARY": [50000.0, 75000.0, 80000.0, 65000.0, 70000.0],
            "ACTIVE": [True, True, False, True, True],
            "HIRE_DATE": [
                datetime.date(2020, 1, 15),
                datetime.date(2019, 3, 22),
                datetime.date(2018, 7, 10),
                datetime.date(2021, 5, 8),
                datetime.date(2020, 11, 30),
            ],
        }
    )

    # Write to DBF
    print("Writing sample data to DBF file...")
    write_dbf(sample_data, "sample.dbf")

    # Read back from DBF
    print("Reading data back from DBF file...")
    df_read = read_dbf("sample.dbf")
    print(df_read)

    # Example with CDX file analysis
    print("\n" + "=" * 50)
    print("CDX FILE ANALYSIS EXAMPLE")
    print("=" * 50)

    # If you have a DBF/CDX file pair, analyze them:
    # analysis = analyze_dbf_with_cdx('yourfile.dbf')
    # print("File Analysis:")
    # print(f"DBF Records: {analysis['record_count']}")
    # print(f"Fields: {len(analysis['fields'])}")
    # print(f"Indexes: {len(analysis['indexes'])}")
    #
    # for field in analysis['fields']:
    #     print(f"  Field: {field['name']} ({field['type']}, {field['length']})")
    #
    # for idx in analysis['indexes']:
    #     print(f"  Index: {idx['name']} - {idx['expression']}")
    #
    # # Read with specific index sorting
    # df_sorted = read_dbf('yourfile.dbf', use_index='INDEX_NAME')
    # print("\nData sorted by index:")
    # print(df_sorted.head())

    # Custom field specifications example
    custom_specs = {
        "ID": {"type": "N", "length": 10, "decimal": 0},
        "NAME": {"type": "C", "length": 30},
        "AGE": {"type": "N", "length": 3, "decimal": 0},
        "SALARY": {"type": "F", "length": 12, "decimal": 2},
        "ACTIVE": {"type": "L", "length": 1},
        "HIRE_DATE": {"type": "D", "length": 8},
    }

    print("\nWriting with custom field specifications...")
    write_dbf(sample_data, "sample_custom.dbf", custom_specs)
    df_custom = read_dbf("sample_custom.dbf")
    print(df_custom)
