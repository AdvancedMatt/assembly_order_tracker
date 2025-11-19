"""
Minimal example showing what's needed to connect to the database in a new script
Copy this pattern to any new script that needs database access
"""

# Required imports for database connection
from database_utils import execute_custom_query
from config_manager import validate_config
import logging

# Configure logging (optional but recommended)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Minimal database connection example"""
    
    # Validate database configuration (optional but recommended)
    if not validate_config():
        logger.error("Database configuration validation failed")
        return
    
    # Example query - replace with your actual SQL
    query = """
        SELECT TOP 5 
            cust_no, 
            order_no, 
            order_date
        FROM R4Order 
        ORDER BY order_date DESC
    """
    
    # Execute query and get results
    try:
        results = execute_custom_query(query)
        logger.info(f"Retrieved {len(results)} records")
        
        # Process results
        for row in results:
            print(f"Customer: {row['cust_no']}, Order: {row['order_no']}, Date: {row['order_date']}")
            
    except Exception as e:
        logger.error(f"Query failed: {e}")

if __name__ == "__main__":
    main()