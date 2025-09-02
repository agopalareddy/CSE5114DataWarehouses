import csv
import os
import hashlib
from typing import Any, List, Dict
from data_warehouse import DataWarehouse


class MyDataWarehouse(DataWarehouse):
    """
    A partitioned data warehouse that distributes data across multiple CSV files
    for improved query performance and scalability.
    
    Design decisions:
    - Hash-based partitioning using the 'id' field for even distribution
    - Each partition stored as a separate CSV file (partition_0.csv, partition_1.csv, etc.)
    - Partition size controls the target number of rows per partition
    - Directory-based storage organization for better file management
    """
    
    def __init__(self, partition_size: int, storage_dir: str):
        """Initialize the partitioned warehouse with partition size and storage directory."""
        self.partition_size = partition_size
        self.storage_dir = storage_dir
        
        # Create storage directory if it doesn't exist
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
    
    def _get_partition_path(self, partition_id: int) -> str:
        """Get the file path for a specific partition."""
        return os.path.join(self.storage_dir, f"partition_{partition_id}.csv")
    
    def _hash_to_partition(self, value: str) -> int:
        """Hash a value to determine its partition ID."""
        # Use MD5 hash for consistent distribution
        hash_obj = hashlib.md5(str(value).encode())
        hash_int = int(hash_obj.hexdigest(), 16)
        
        # Calculate optimal number of partitions based on expected data size
        # Use partition_size to determine how many partitions we might need
        # For efficiency, use a reasonable number of partitions (e.g., 10-20)
        optimal_partitions = max(1, min(20, 10000 // self.partition_size))
        
        return hash_int % optimal_partitions
    
    def _get_all_partition_files(self) -> List[str]:
        """Get all existing partition files."""
        partition_files = []
        for i in range(100):  # Check up to 100 partitions
            partition_path = self._get_partition_path(i)
            if os.path.exists(partition_path):
                partition_files.append(partition_path)
            else:
                break
        return partition_files
    
    def _read_partition_data(self, partition_path: str) -> List[Dict[str, Any]]:
        """Read all data from a partition file."""
        if not os.path.exists(partition_path):
            return []
        
        try:
            with open(partition_path, 'r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                return list(reader)
        except (IOError, csv.Error):
            return []
    
    def _write_partition_data(self, partition_path: str, data: List[Dict[str, Any]]) -> None:
        """Write all data to a partition file."""
        if not data:
            # If no data, remove the file if it exists
            if os.path.exists(partition_path):
                os.remove(partition_path)
            return
        
        headers = list(data[0].keys()) if data else []
        with open(partition_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
    
    def _append_to_partition(self, partition_path: str, data: Dict[str, Any]) -> None:
        """Append a single row to a partition file."""
        file_exists = os.path.exists(partition_path)
        
        if not file_exists:
            # Create new partition file with headers
            with open(partition_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=data.keys())
                writer.writeheader()
                writer.writerow(data)
        else:
            # Append to existing partition
            with open(partition_path, 'a', newline='', encoding='utf-8') as file:
                # Get existing headers
                existing_data = self._read_partition_data(partition_path)
                if existing_data:
                    headers = list(existing_data[0].keys())
                else:
                    headers = list(data.keys())
                
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writerow(data)
    
    def add_data(self, data: Dict[str, Any]) -> None:
        """
        Add a row of data to the appropriate partition based on hash of the 'id' field.
        
        Args:
            data (Dict[str, Any]): A dictionary representing a row of data.
        """
        if not data or 'id' not in data:
            return
        
        # Determine partition based on ID hash
        partition_id = self._hash_to_partition(data['id'])
        partition_path = self._get_partition_path(partition_id)
        
        # Append data to the appropriate partition
        self._append_to_partition(partition_path, data)

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        """
        Update the first row matching the key column/value across all partitions.
        
        Args:
            key_column (str): The column to match for the update.
            key_value (Any): The value to match in the key column.
            updated_data (Dict[str, Any]): A dictionary with updated column values.
        """
        partition_files = self._get_all_partition_files()
        
        for partition_path in partition_files:
            partition_data = self._read_partition_data(partition_path)
            updated = False
            
            for row in partition_data:
                if row.get(key_column) == str(key_value):
                    row.update(updated_data)
                    updated = True
                    break  # Update only the first matching row
            
            if updated:
                self._write_partition_data(partition_path, partition_data)
                return  # Stop after first update

    def delete_data(self, key_column: str, key_value: Any) -> None:
        """
        Delete all rows matching the key column/value across all partitions.
        
        Args:
            key_column (str): The column to match for the deletion.
            key_value (Any): The value to match in the key column.
        """
        partition_files = self._get_all_partition_files()
        
        for partition_path in partition_files:
            partition_data = self._read_partition_data(partition_path)
            filtered_data = [row for row in partition_data if row.get(key_column) != str(key_value)]
            
            # Only rewrite if data was actually deleted
            if len(filtered_data) != len(partition_data):
                self._write_partition_data(partition_path, filtered_data)

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        """
        Query data from all partitions for rows matching the key column values.
        
        Args:
            key_column (str): The column to match for the query.
            keys (List[Any]): A list of values to search for in the key column.
        
        Returns:
            List[Dict[str, Any]]: The query results from all partitions.
        """
        if not keys:
            return []
        
        partition_files = self._get_all_partition_files()
        str_keys = [str(key) for key in keys]
        results = []
        
        for partition_path in partition_files:
            partition_data = self._read_partition_data(partition_path)
            for row in partition_data:
                if row.get(key_column) in str_keys:
                    results.append(row)
        
        return results
