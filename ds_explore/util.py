

def str2float(x):
    try: return float(x)
    except: return None

def str2int(x):
    try: return int(x)
    except: return None


# Some columns have hive keywords for their names.  Specifically the
# "format" column in Jade
HIVE_KEYWORDS = ["format", "table", "create", "insert", "overwrite", "where"]



def get_tables_to_join():
    return TABLES_TO_JOIN

def get_table_schema_w_types(table):
    return [
        (l.split('\t')[0], l.split('\t')[1])
        for l in open(SCHEMA_DIR + table + ".schema","r").readlines()
    ]

def get_top_errors(args):
    records = [l.strip().split() for l in  open(args.error_count_fname,'r').readlines() if l.strip()]
    pfcode_count_dict = {}
    for r in records:
        date, pfcode, ct = r
        try: pfcode_count_dict[pfcode] += int(ct)
        except: pfcode_count_dict[pfcode] = int(ct)
    pfcode_count = pfcode_count_dict.items()
    pfcode_count.sort(
        key=lambda r:r[1],
        reverse=True
    )
    codes = [r[0] for r in pfcode_count]
    codes.remove(PASSING_PFCODE)
    top_errors = codes[:args.num_top_errors]
    return top_errors

def groupby(records, fields):
    assert isinstance(records, list)
    assert isinstance(fields, list)
    assert all(isinstance(x, str) for x in fields)
    grpd = {}
    for r in records:
        key = tuple([r[f] for f in fields])
        try: grpd[key].append(r)
        except: grpd[key] = [r]
    return grpd


