"""
Merges several Hive tables on a common key into a single large one.

Note
* Column names in output table are pre-pended w name of table they came from.
* Does inner join
* Expects every table to have different name
"""

TABLES_TO_JOIN = ['sample_07', 'sample_08']
JOIN_COLUMN = 'code'
JOINED_TABLE_NAME = 'joined'

import os

def get_schema(tabname):
    """ Return schema as  (colname, type) list """
    as_txt = os.popen("""hive -e "describe %s" """ % tabname).read()
    return [(l.split()[0], l.split()[1]) for l in as_txt.split('\n') if l]

def get_join_query(tables_to_join, schemas, join_column):
    parts = []
    tables_wo_db = [t.split('.')[-1] for t in tables_to_join]
    
    parts.append('CREATE TABLE %s AS' % JOINED_TABLE_NAME)
    
    cols_to_select_by_table = [
        [(tab + '.' + colname + ' as ' + tab + '_' + colname) for colname in schema]
        for tab, schema
        in zip(tables_wo_db, schemas)
    ]
    cols_to_select = reduce(lambda l1, l2: l1+l2, cols_to_select_by_table)
    parts.append('SELECT %s' % ','.join(cols_to_select))
    
    parts.append('FROM %s %s' % (tables_to_join[0], tables_wo_db[0]))
    for full_tab, tab in zip(tables_to_join, tables_wo_db)[1:]:
        parts.append('JOIN %s %s' % (full_tab, tab))
    for full_tab, tab in zip(tables_to_join, tables_wo_db)[1:]:
        parts.append('ON %(tab1)s.%(joincol)s = %(tab2)s.%(joincol)s' % 
            {'tab1':tab, 'tab2':tables_wo_db[0], 'joincol':join_column})
    
    return '\n'.join(parts)

def main():
    schemas_w_types = [get_schema(t) for t in TABLES_TO_JOIN]
    schemas = [[colname for colname, type in schema] for schema in schemas_w_types]
    assert(all((JOIN_COLUMN in s) for s in schemas))
    query = get_join_query(TABLES_TO_JOIN, schemas, JOIN_COLUMN)
    os.system('hive -e "%s"' % query)

if __name__=='__main__':
    main()
    

