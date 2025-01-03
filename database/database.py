import logging
from typing import List, Dict, Any, Optional
import pandas as pd
import mysql.connector
from mysql.connector import Error
from database.db_config import DatabaseConfig

logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self):
        self.config = DatabaseConfig()
        
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            
        Returns:
            pd.DataFrame: Query results
        """
        try:
            conn = mysql.connector.connect(**self.config.get_connection_params())
            cursor = conn.cursor(dictionary=True)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            results = cursor.fetchall()
            return pd.DataFrame(results)
            
        except Error as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a specific table"""
        query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{self.config.DATABASE}' 
        AND TABLE_NAME = '{table_name}'
        """
        df = self.execute_query(query)
        return df['COLUMN_NAME'].tolist()
    
    def get_distinct_values(self, table: str, column: str) -> List[Any]:
        """Get distinct values from a specific column"""
        query = f"SELECT DISTINCT {column} FROM {table} ORDER BY {column}"
        df = self.execute_query(query)
        return df[column].tolist()