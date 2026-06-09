import os
from dotenv import load_dotenv
from jamaibase import JamAI, protocol as p
from jamaibase.exceptions import ResourceNotFoundError, JamaiException
import sys # To exit script on critical errors

# --- 1. Configuration ---
load_dotenv()

# --- JamAI Connection Details ---
PROJECT_ID = "proj_d51957697af3bcec339092cb" 

JAMAI_API_BASE = os.getenv("JAMAI_API_BASE", "http://localhost:6969/api") # Optional: Set via env var or default

# --- Table Details ---
# IMPORTANT: Specify the EXACT source table ID you want to clone the schema from
SOURCE_TABLE_ID = "Input_table_v3" 
TARGET_TABLE_ID = "Input_table_template"
TABLE_TYPE = p.TableType.action # Assuming it's an action table like the source

print("Initializing JamAI Client...")
try:
    jamai = JamAI(project_id=PROJECT_ID, api_base=JAMAI_API_BASE)
    print(f"Client Initialized. Project ID: {jamai.project_id}, API Base: {jamai.api_base}")
except JamaiException as e:
    print(f"Failed to initialize JamAI client: {e}")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred during initialization: {e}")
    sys.exit(1)

# --- 3. Check if Source Table Exists ---
print(f"\n--- Checking if source table '{SOURCE_TABLE_ID}' exists ---")
try:
    jamai.table.get_table(table_type=TABLE_TYPE, table_id=SOURCE_TABLE_ID)
    print(f"✓ Source table '{SOURCE_TABLE_ID}' found.")
except ResourceNotFoundError:
    print(f"✗ Error: Source table '{SOURCE_TABLE_ID}' not found.")
    print("Cannot clone schema from a non-existent table. Please verify the SOURCE_TABLE_ID.")
    sys.exit(1)
except JamaiException as e:
    print(f"✗ Error checking for source table '{SOURCE_TABLE_ID}': {e}")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred while checking the source table: {e}")
    sys.exit(1)

# --- 4. Check for Existing Target Table and Delete if Found ---
print(f"\n--- Checking for existing target table '{TARGET_TABLE_ID}' ---")
try:
    # Attempt to get the target table metadata
    jamai.table.get_table(table_type=TABLE_TYPE, table_id=TARGET_TABLE_ID)
    # If the above line doesn't raise ResourceNotFoundError, the table exists
    print(f"Target table '{TARGET_TABLE_ID}' already exists. Attempting to delete it first...")
    try:
        jamai.table.delete_table(table_type=TABLE_TYPE, table_id=TARGET_TABLE_ID)
        print(f"✓ Successfully deleted existing table '{TARGET_TABLE_ID}'.")
        # Optional: Add a small delay if needed, though usually not necessary for schema operations
        # import time
        # time.sleep(1)
    except JamaiException as e_del:
        print(f"✗ Error deleting existing table '{TARGET_TABLE_ID}': {e_del}")
        print("Exiting script as the target table cannot be prepared.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred while deleting the target table: {e}")
        sys.exit(1)

except ResourceNotFoundError:
    # This is the expected case if the target table doesn't exist yet
    print(f"Target table '{TARGET_TABLE_ID}' not found. Will proceed to create.")
except JamaiException as e_get:
    # Handle other errors during the target table check
    print(f"✗ Error checking for target table '{TARGET_TABLE_ID}': {e_get}")
    print("Exiting script due to check error.")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred while checking the target table: {e}")
    sys.exit(1)

# --- 5. Duplicate Schema (without data) ---
print(f"\n--- Attempting to duplicate schema from '{SOURCE_TABLE_ID}' to '{TARGET_TABLE_ID}' (without data) ---")
try:
    response = jamai.table.duplicate_table(
        table_type=TABLE_TYPE,
        table_id_src=SOURCE_TABLE_ID,
        table_id_dst=TARGET_TABLE_ID,
        include_data=False  # <<< This is the crucial parameter
    )
    # Check response structure - adjust if needed based on actual SDK response
    if hasattr(response, 'id') and response.id == TARGET_TABLE_ID:
         print(f"✓ Successfully created '{TARGET_TABLE_ID}' with the schema from '{SOURCE_TABLE_ID}'.")
         print(f"   Table ID: {response.id}")
         # You could potentially print more details from the response if available
         # print(f"   Description: {response.meta.get('description', 'N/A')}")
    else:
         # Fallback success message if response structure is different
         print(f"✓ Schema duplication request completed for '{TARGET_TABLE_ID}'. Please verify in JamAI.")
         # print(response) # Optional: print the raw response for debugging

except JamaiException as e_dup:
    print(f"✗ Error duplicating table schema: {e_dup}")
    print(f"   Failed to create '{TARGET_TABLE_ID}' from '{SOURCE_TABLE_ID}'.")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred during schema duplication: {e}")
    sys.exit(1)

print("\n--- Script Finished ---")

# Optional: Close the client if desired
# jamai.close()