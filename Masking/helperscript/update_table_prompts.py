import argparse
from dotenv import load_dotenv
import os
from jamaibase import JamAI, protocol as p
import json
from loguru import logger

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID", "")
API_BASE = os.getenv("JAMAI_API_BASE", "http://localhost:6969/api")

def main(args):
    table_id = args.table_id
    # handle input_dir in both ending with / or not
    if args.input_dir[-1] == "/":
        input_dir = args.input_dir[:-1]
    else:
        input_dir = args.input_dir

    # Initialize the JamAI client
    jamai = JamAI(project_id=args.project_id, api_base=API_BASE)

    # Get the table
    table = jamai.table.get_table(table_type=p.TableType.action, table_id=table_id)
    if table is None:
        raise ValueError(f"Table {table_id} not found.")

    update_col_map = {}
    # Update the prompts for each column
    for col in table.cols:
        # only output columns have prompts
        if col.gen_config:
            logger.info(f"Processing column: {col.id}")
            gen_config = col.gen_config
            # now read the prompt, system_prompt, and params
            with open(f"{input_dir}/{col.id}_prompt.txt", "r", encoding="utf-8") as f:
                prompt = f.read()
            with open(f"{input_dir}/{col.id}_system_prompt.txt", "r", encoding="utf-8") as f:
                system_prompt = f.read()
            with open(f"{input_dir}/{col.id}_params.json", "r", encoding="utf-8") as f:
                params = json.load(f)
            gen_config.prompt = prompt
            gen_config.system_prompt = system_prompt
            gen_config.temperature = params["temperature"]
            gen_config.top_p = params["top-p"]
            gen_config.max_tokens = params["max_tokens"]
            update_col_map[col.id] = gen_config

    logger.info(f"Updating prompts for columns: {update_col_map.keys()}")
    jamai.table.update_gen_config(
        table_type=p.TableType.action,
        request=p.GenConfigUpdateRequest(
            table_id = table_id,
            column_map=update_col_map,
        )
    )



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-id", help="Table ID to be updated (ensure the table is already with proper column setups)", type=str, required=True)
    parser.add_argument("--input-dir", help="Input directory with the updated prompts", type=str, required=True)
    parser.add_argument("--project-id", help="Project ID", type=str, required=True)
    args = parser.parse_args()
    main(args)