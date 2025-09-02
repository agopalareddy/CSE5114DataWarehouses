import csv
import os
import hashlib
from typing import Any, List, Dict, Optional, Set
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
        
        # In-memory partition metadata cache for performance optimization
        self._partition_cache = {
            'file_exists': {},      # Track which partition files exist
            'headers': {},          # Cache headers for each partition
            'row_counts': {},       # Track approximate row counts per partition
            'last_accessed': {}     # Track last access time for cache management
        }
    
    def _get_partition_path(self, partition_id: int) -> str:
        """Get the file path for a specific partition."""
        return os.path.join(self.storage_dir, f"partition_{partition_id}.csv")
    
    def _update_partition_cache(self, partition_path: str, headers: Optional[List[str]] = None, 
                               exists: Optional[bool] = None, row_count_delta: int = 0) -> None:
        """Update the partition metadata cache."""
        import time
        
        if headers is not None:
            self._partition_cache['headers'][partition_path] = headers
        
        if exists is not None:
            self._partition_cache['file_exists'][partition_path] = exists
        
        if row_count_delta != 0:
            current_count = self._partition_cache['row_counts'].get(partition_path, 0)
            self._partition_cache['row_counts'][partition_path] = max(0, current_count + row_count_delta)
        
        self._partition_cache['last_accessed'][partition_path] = time.time()
    
    def _get_cached_headers(self, partition_path: str) -> Optional[List[str]]:
        """Get cached headers for a partition, reading from file if not cached."""
        # Check cache first
        cached_headers = self._partition_cache['headers'].get(partition_path)
        if cached_headers is not None:
            return cached_headers
        
        # If not cached and file exists, read just the first line to get headers
        if os.path.exists(partition_path):
            try:
                with open(partition_path, 'r', newline='', encoding='utf-8') as file:
                    reader = csv.reader(file)
                    headers = next(reader, [])
                    if headers:
                        self._update_partition_cache(partition_path, headers=headers, exists=True)
                        return headers
            except (IOError, csv.Error, StopIteration):
                pass
        
        return None
    
    def _partition_exists_cached(self, partition_path: str) -> bool:
        """Check if a partition exists using cache when possible."""
        # Check cache first
        cached_exists = self._partition_cache['file_exists'].get(partition_path)
        if cached_exists is not None:
            return cached_exists
        
        # Check filesystem and update cache
        exists = os.path.exists(partition_path)
        self._update_partition_cache(partition_path, exists=exists)
        return exists
    
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
        """Get all existing partition files using cached existence checks."""
        partition_files = []
        # Check all possible partitions since they may not be sequential
        # Use the same range as the partition calculation
        optimal_partitions = max(1, min(20, 10000 // self.partition_size))
        for i in range(optimal_partitions):
            partition_path = self._get_partition_path(i)
            if self._partition_exists_cached(partition_path):
                partition_files.append(partition_path)
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
    
    def _stream_partition_data(self, partition_path: str):
        """Stream partition data row by row instead of loading all into memory."""
        if not os.path.exists(partition_path):
            return
        
        try:
            with open(partition_path, 'r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    yield row
        except (IOError, csv.Error):
            return
    
    def _write_partition_data(self, partition_path: str, data: List[Dict[str, Any]]) -> None:
        """Write all data to a partition file and update cache."""
        if not data:
            # If no data, remove the file if it exists
            if os.path.exists(partition_path):
                os.remove(partition_path)
            # Update cache to reflect file removal
            self._update_partition_cache(partition_path, exists=False, row_count_delta=-self._partition_cache['row_counts'].get(partition_path, 0))
            # Clear headers cache for removed file
            self._partition_cache['headers'].pop(partition_path, None)
            return
        
        headers = list(data[0].keys()) if data else []
        with open(partition_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        
        # Update cache with new file info
        old_count = self._partition_cache['row_counts'].get(partition_path, 0)
        new_count = len(data)
        self._update_partition_cache(partition_path, headers=headers, exists=True, 
                                   row_count_delta=new_count - old_count)
    
    def _append_to_partition(self, partition_path: str, data: Dict[str, Any]) -> None:
        """Append a single row to a partition file."""
        file_exists = self._partition_exists_cached(partition_path)
        
        if not file_exists:
            # Create new partition file with headers
            headers = list(data.keys())
            with open(partition_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()
                writer.writerow(data)
            
            # Update cache with new file info
            self._update_partition_cache(partition_path, headers=headers, exists=True, row_count_delta=1)
        else:
            # Append to existing partition using cached headers
            headers = self._get_cached_headers(partition_path)
            if not headers:
                # Fallback if cache miss - use data keys
                headers = list(data.keys())
            
            with open(partition_path, 'a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writerow(data)
            
            # Update cache with row count increment
            self._update_partition_cache(partition_path, row_count_delta=1)
    
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
        Uses streaming to find matches efficiently before loading partition for update.
        
        Args:
            key_column (str): The column to match for the update.
            key_value (Any): The value to match in the key column.
            updated_data (Dict[str, Any]): A dictionary with updated column values.
        """
        partition_files = self._get_all_partition_files()
        str_key_value = str(key_value)
        
        for partition_path in partition_files:
            # First, use streaming to check if the partition contains matching data
            found_match = False
            for row in self._stream_partition_data(partition_path):
                if row.get(key_column) == str_key_value:
                    found_match = True
                    break
            
            # Only load the full partition if we found a match
            if found_match:
                partition_data = self._read_partition_data(partition_path)
                updated = False
                
                for row in partition_data:
                    if row.get(key_column) == str_key_value:
                        row.update(updated_data)
                        updated = True
                        break  # Update only the first matching row
                
                if updated:
                    self._write_partition_data(partition_path, partition_data)
                    return  # Stop after first update

    def delete_data(self, key_column: str, key_value: Any) -> None:
        """
        Delete all rows matching the key column/value across all partitions.
        Uses streaming to find matches efficiently before loading partition for deletion.
        
        Args:
            key_column (str): The column to match for the deletion.
            key_value (Any): The value to match in the key column.
        """
        partition_files = self._get_all_partition_files()
        str_key_value = str(key_value)
        
        for partition_path in partition_files:
            # First, use streaming to check if the partition contains matching data
            found_match = False
            for row in self._stream_partition_data(partition_path):
                if row.get(key_column) == str_key_value:
                    found_match = True
                    break
            
            # Only load the full partition if we found a match
            if found_match:
                partition_data = self._read_partition_data(partition_path)
                filtered_data = [row for row in partition_data if row.get(key_column) != str_key_value]
                
                # Only rewrite if data was actually deleted
                if len(filtered_data) != len(partition_data):
                    self._write_partition_data(partition_path, filtered_data)

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        """
        Query data from partitions for rows matching the key column values.
        Uses partition-aware querying for 'id' column and memory-efficient streaming.
        
        Args:
            key_column (str): The column to match for the query.
            keys (List[Any]): A list of values to search for in the key column.
        
        Returns:
            List[Dict[str, Any]]: The query results from relevant partitions.
        """
        if not keys:
            return []
        
        results = []
        str_keys = set(str(key) for key in keys)  # Use set for faster lookups
        
        # Optimize for ID-based queries - search only relevant partitions
        if key_column == 'id':
            target_partitions = set()
            for key in str_keys:
                partition_id = self._hash_to_partition(key)
                partition_path = self._get_partition_path(partition_id)
                target_partitions.add(partition_path)
            
            for partition_path in target_partitions:
                # Use streaming to avoid loading entire partition into memory
                for row in self._stream_partition_data(partition_path):
                    if row.get(key_column) in str_keys:
                        results.append(row)
        else:
            # Fall back to searching all partitions for non-ID queries with streaming
            partition_files = self._get_all_partition_files()
            for partition_path in partition_files:
                # Use streaming to avoid loading entire partition into memory
                for row in self._stream_partition_data(partition_path):
                    if row.get(key_column) in str_keys:
                        results.append(row)
        
        return results
