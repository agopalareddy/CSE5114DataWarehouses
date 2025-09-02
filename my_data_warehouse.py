from typing import Any, List, Dict
from data_warehouse import DataWarehouse


class MyDataWarehouse(DataWarehouse):
    def __init__(self, partition_size: int, storage_dir: str):
        """Initialize the partitioned warehouse with partition size and storage directory."""
        self.partition_size = partition_size
        self.storage_dir = storage_dir
    
    def add_data(self, data: Dict[str, Any]) -> None:
        # Implementation here
        raise NotImplementedError("This method is not implemented yet.")

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        # Implementation here
        raise NotImplementedError("This method is not implemented yet.")

    def delete_data(self, key_column: str, key_value: Any) -> None:
        # Implementation here
        raise NotImplementedError("This method is not implemented yet.")

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        # Implementation here
        raise NotImplementedError("This method is not implemented yet.")
