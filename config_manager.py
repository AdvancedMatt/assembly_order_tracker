"""
Configuration Manager for Customer Dashboard
Handles configuration file and database credentials
"""

import os
import sys
import logging
import configparser

# Configure logging
logger = logging.getLogger(__name__)

# Get script directory for finding config.ini
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    SCRIPT_DIR = os.path.dirname(sys.executable)
else:
    # Running as a script
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Import the SQL password decryption function
# Note: Import here to avoid circular imports
def get_sql_password():
    """Import and use the get_sql_password function from functions.py"""
    from functions import get_sql_password as _get_sql_password
    return _get_sql_password()

class DatabaseConfig:
    """Database configuration handler"""
    
    def __init__(self):
        """Initialize database configuration from config.ini file"""
        self.server = None
        self.database = None
        self.username = None
        self.password = None
        
        # Load configuration from INI file
        config_path = os.path.join(SCRIPT_DIR, 'config.ini')
        
        if not os.path.exists(config_path):
            logger.warning(f"Configuration file not found: {config_path}")
            logger.warning("Database features will be disabled")
            return
        
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            
            if 'Database' in config:
                self.server = config['Database'].get('server')
                self.database = config['Database'].get('database')
                self.username = config['Database'].get('username')
                logger.info(f"Loaded database config from {config_path}")
            else:
                logger.warning("No [Database] section found in config.ini")
                
        except Exception as e:
            logger.warning(f"Failed to read config.ini: {e}")
            return
        
        # Get password from encrypted file
        try:
            self.password = get_sql_password()
            if not self.password:
                logger.warning("SQL password decryption returned empty/None - database features will be disabled")
        except FileNotFoundError as e:
            logger.warning(f"SQL password file not found: {e} - database features will be disabled")
            self.password = None
        except Exception as e:
            logger.warning(f"Failed to decrypt SQL password: {e} - database features will be disabled")
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
                logger.error("For other fields, check your config.ini file")
            else:
                logger.error(f"Missing database configuration: {', '.join(missing_fields)}")
                logger.error("Please check your config.ini file")
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
