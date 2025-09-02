import csv
import os
from typing import Any, List, Dict
from data_warehouse import DataWarehouse


class NaiveCSVWarehouse(DataWarehouse):
    def __init__(self, csv_file: str):
        """Initialize the CSV warehouse with a filename."""
        self.csv_file = csv_file
    
    def add_data(self, data: Dict[str, Any]) -> None:
        """
        Add a row of data to the CSV file.
        
        Args:
            data (Dict[str, Any]): A dictionary representing a row of data.
        """
        file_exists = os.path.exists(self.csv_file)
        
        # If file doesn't exist, create it with headers
        if not file_exists:
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as file:
                if data:
                    writer = csv.DictWriter(file, fieldnames=data.keys())
                    writer.writeheader()
                    writer.writerow(data)
        else:
            # Append data to existing file
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as file:
                if data:
                    # Read existing headers to maintain consistency
                    existing_headers = self._get_headers()
                    if existing_headers:
                        writer = csv.DictWriter(file, fieldnames=existing_headers)
                        writer.writerow(data)
                    else:
                        # File exists but is empty, write headers first
                        writer = csv.DictWriter(file, fieldnames=data.keys())
                        writer.writeheader()
                        writer.writerow(data)

    def _get_headers(self) -> List[str]:
        """Get the headers from the CSV file."""
        if not os.path.exists(self.csv_file):
            return []
        
        try:
            with open(self.csv_file, 'r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file)
                headers = next(reader, [])
                return headers
        except (StopIteration, IOError):
            return []

    def _read_all_data(self) -> List[Dict[str, Any]]:
        """Read all data from the CSV file."""
        if not os.path.exists(self.csv_file):
            return []
        
        try:
            with open(self.csv_file, 'r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                return list(reader)
        except (IOError, csv.Error):
            return []

    def _write_all_data(self, data: List[Dict[str, Any]]) -> None:
        """Write all data to the CSV file."""
        if not data:
            # If no data, create empty file or clear existing file
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as file:
                pass
            return
        
        headers = list(data[0].keys()) if data else []
        with open(self.csv_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        """
        Update the first row matching the key column/value.
        
        Args:
            key_column (str): The column to match for the update.
            key_value (Any): The value to match in the key column.
            updated_data (Dict[str, Any]): A dictionary with updated column values.
        """
        all_data = self._read_all_data()
        updated = False
        
        for row in all_data:
            if row.get(key_column) == str(key_value):
                row.update(updated_data)
                updated = True
                break  # Update only the first matching row
        
        if updated:
            self._write_all_data(all_data)

    def delete_data(self, key_column: str, key_value: Any) -> None:
        """
        Delete all rows matching the key column/value.
        
        Args:
            key_column (str): The column to match for the deletion.
            key_value (Any): The value to match in the key column.
        """
        all_data = self._read_all_data()
        filtered_data = [row for row in all_data if row.get(key_column) != str(key_value)]
        
        # Only rewrite if data was actually deleted
        if len(filtered_data) != len(all_data):
            self._write_all_data(filtered_data)

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        """
        Query data from the CSV file.
        
        Args:
            key_column (str): The column to match for the query.
            keys (List[Any]): A list of values to search for in the key column.
        
        Returns:
            List[Dict[str, Any]]: The query results.
        """
        if not keys:
            return []
        
        all_data = self._read_all_data()
        str_keys = [str(key) for key in keys]
        
        results = []
        for row in all_data:
            if row.get(key_column) in str_keys:
                results.append(row)
        
        return results
