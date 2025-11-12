"""
Configuration Manager for Customer Dashboard
Handles environment variables and database credentials
"""

import os
import logging
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)

# Import the SQL password decryption function
# Note: Import here to avoid circular imports
def get_sql_password():
    """Import and use the get_sql_password function from functions.py"""
    from functions import get_sql_password as _get_sql_password
    return _get_sql_password()

# Load environment variables from .env file
load_dotenv()

class DatabaseConfig:
    """Database configuration handler"""
    
    def __init__(self):
        """Initialize database configuration from environment variables and encrypted password"""
        self.server = os.getenv('DB_SERVER')
        self.database = os.getenv('DB_DATABASE')
        self.username = os.getenv('DB_USERNAME')
        # Get password from encrypted file instead of environment variable
        try:
            self.password = get_sql_password()
        except Exception as e:
            logger.error(f"Failed to decrypt SQL password: {e}")
            self.password = None
    
    def validate(self) -> bool:
        """
        Validate that all required database configuration is present.
        
        Returns:
            True if all required fields are present, False otherwise
        """
        required_fields = {
            'server': self.server,
            'database': self.database,
            'username': self.username,
            'password': self.password
        }
        
        missing_fields = [field for field, value in required_fields.items() if not value]
        
        if missing_fields:
            if 'password' in missing_fields:
                logger.error(f"Missing database configuration: {', '.join(missing_fields)}")
                logger.error("Password error - please check SQL_PASSWORD_PATH and SQL_PASSWORD_KEY_PATH files")
                logger.error("For other fields, check your .env file or environment variables")
            else:
                logger.error(f"Missing database configuration: {', '.join(missing_fields)}")
                logger.error("Please check your .env file or environment variables")
            return False
        
        return True
    
    def get_connection_string(self) -> str:
        """
        Build and return ODBC connection string for MS SQL Server.
        
        Returns:
            Formatted connection string
            
        Raises:
            ValueError: If configuration is invalid
        """
        if not self.validate():
            raise ValueError("Invalid database configuration")
        
        connection_string = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            "TrustServerCertificate=yes;"
        )
        
        return connection_string

# Create global instance
db_config = DatabaseConfig()


def validate_config() -> bool:
    """
    Validate the database configuration.
    
    Returns:
        True if configuration is valid
    """
    return db_config.validate()
