import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import and run the test_db_system main function
from DB.NEW_KT_DB.test_db_system import main

if __name__ == "__main__":
    main()
