"""
Database utilities for Customer Dashboard
Handles MS SQL Server database connections and queries using pyodbc
"""

import pyodbc
import logging
from typing import List, Dict, Any, Optional
from config_manager import db_config

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """
    Database connection manager for MS SQL Server.
    Handles connection lifecycle and provides query execution methods.
    """
    
    def __init__(self):
        """Initialize database connection manager"""
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        Establish connection to the database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            connection_string = db_config.get_connection_string()
            logger.debug(f"Attempting database connection...")
            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            logger.info("Database connection established successfully")
            return True
        except pyodbc.InterfaceError as e:
            logger.error(f"Database interface error: {e}")
            logger.error("This usually means ODBC driver is missing or misconfigured")
            return False
        except pyodbc.OperationalError as e:
            logger.error(f"Database operational error: {e}")
            logger.error("This could mean: wrong server name, network issues, or authentication failure")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            return False
    
    def disconnect(self):
        """Close database connection and cursor"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results as list of dictionaries.
        
        Args:
            query: SQL query string
            params: Optional tuple of parameters for parameterized query
            
        Returns:
            List of dictionaries with column names as keys
            
        Raises:
            Exception: If query execution fails
        """
        try:
            logger.debug(f"Executing query (first 100 chars): {query[:100]}...")
            if params:
                logger.debug(f"Query parameters: {params}")
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            # Get column names from cursor description
            if self.cursor.description:
                columns = [column[0] for column in self.cursor.description]
                
                # Fetch all rows and convert to list of dictionaries
                results = []
                for row in self.cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                logger.info(f"Query executed successfully, returned {len(results)} rows")
                return results
            else:
                logger.warning("Query executed but returned no result set (possibly a non-SELECT query)")
                return []
            
        except pyodbc.ProgrammingError as e:
            logger.error(f"SQL programming error: {e}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Parameters: {params}")
            raise
        except pyodbc.DataError as e:
            logger.error(f"SQL data error: {e}")
            logger.error(f"Query: {query}")
            raise
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Query: {query}")
            raise
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()

def execute_custom_query(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute a custom SELECT query.
    This provides a modular way to run any query as the script grows.
    
    Args:
        query: SQL query string
        params: Optional tuple of parameters for parameterized query
        
    Returns:
        List of dictionaries with query results
    """
    try:
        with DatabaseConnection() as db:
            results = db.execute_query(query, params)
            return results
    except Exception as e:
        logger.error(f"Custom query execution failed: {e}")
        return []
