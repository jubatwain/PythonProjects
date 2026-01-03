import json
import yaml
import pickle
from pathlib import Path
from typing import Any, Dict, List
import logging

logger = logging.getLogger(__name__)

class FileHandler:
    """Handle file operations"""
    
    @staticmethod
    def read_json(filepath: Path) -> Dict:
        """Read JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading JSON {filepath}: {e}")
            return {}
    
    @staticmethod
    def write_json(data: Any, filepath: Path) -> bool:
        """Write to JSON file"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            logger.error(f"Error writing JSON {filepath}: {e}")
            return False
    
    @staticmethod
    def read_pickle(filepath: Path) -> Any:
        """Read pickle file"""
        try:
            with open(filepath, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.error(f"Error reading pickle {filepath}: {e}")
            return None
    
    @staticmethod
    def write_pickle(data: Any, filepath: Path) -> bool:
        """Write to pickle file"""
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)
            return True
        except Exception as e:
            logger.error(f"Error writing pickle {filepath}: {e}")
            return False
    
    @staticmethod
    def get_files(directory: Path, pattern: str = "*.pdf") -> List[Path]:
        """Get all files matching pattern in directory"""
        try:
            return list(directory.glob(pattern))
        except Exception as e:
            logger.error(f"Error getting files from {directory}: {e}")
            return []