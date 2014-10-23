import subprocess as sp
import os, getpass
import util

DATA_DIR = "./"

SCHEMA_DIR = "schema/"

TABLE_FNAME = DATA_DIR + "tables.tsv"

DESCRIBE_TEMPLATE = """hive -e "describe %(table)s" > %(schema_dir)s/%(table)s.schema""" 
DESCRIBE_SCRIPT_NAME = "describe.sh"

def write_name_files():
    tables = util.get_tables_to_join()
    parts = ["mkdir %s" % SCHEMA_DIR]
    for t in tables:
        line = DESCRIBE_TEMPLATE % {"table":t, "schema_dir":SCHEMA_DIR}
        parts.append(line)
    open(DESCRIBE_SCRIPT_NAME, "w").write(
        "\n".join(parts)
    )
    os.system("sh " + DESCRIBE_SCRIPT_NAME)

write_name_files()

