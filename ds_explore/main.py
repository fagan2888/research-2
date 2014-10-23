"""
This script has

1) Count all error code occurences

2) Create "target table" that 


"""

import sys, time

# Data about test results
TEST_RESULT_TABLE = "venusq.association"
TEST_RESULT_KEY_COLUMN = "slidersn"
PFCODE_COLUMN = "pfcode"
PASSING_PFCODE = "0000"

# Counting pfcodes
PFCODE_COUNT_FNAME = "pfcode_counts.csv"

# Creating target table
TARGET_TABLE_NAME="field.pfcode_serials_changes"

FEATURE_WEIGHTS_FILENAME = "top_Features.csv"

TABLES_TO_JOIN_LIST = """
# Parametric tables
sldr.bcslider
sldr.decodet_deduped
sldr.decoquasi
sldr.etchdepth
sldr.flatness_deduped
sldr.jade_deduped
sldr.lapfinalsubphase_deduped
sldr.lapsubphaseslider
sldr.sliderbin
sldr.sliderbinhist
sldr.sliderdefecthist
sldr.sliderdefectjrnl
sldr.sliderhist_deduped
sldr.sliderquasi

# Nakagawa stream
#hgaqe.gtsd_cbf_repartitioned
#usercontrib.nakagawa_stream_aug18

"""

TABLES_TO_JOIN = [
    t.strip()
    for t in TABLES_TO_JOIN_LIST.strip().split("\n")
    if t.strip() and not t.startswith("#")
]

# Some columns have hive keywords for their names.  Specifically the
# "format" column in Jade
HIVE_KEYWORDS = ["format", "table", "create", "insert", "overwrite", "where"]


def get_populate_pfcode_probability_map(args):
    counts_by_pfcode = {}
    for line in open(args.error_count_fname,'r').readlines():
        r = dict(zip(util.ERROR_COUNT_SCHEMA, line.split(util.DELIM)))
        counts_by_pfcode[r["pfcode"]] = counts_by_pfcode.setdefault(r["pfcode"],0) + int(r["count"])
    n_passing = counts_by_pfcode[util.PASSING_PFCODE]
    del counts_by_pfcode[util.PASSING_PFCODE]
    pfcode_count_pairs = counts_by_pfcode.items()
    pfcode_count_pairs.sort(key=lambda pair: int(pair[1]), reverse=True)
    n_others = sum(ct for pfcd, ct in pfcode_count_pairs[args.num_top_errors+1:])
    pfcode_count_pairs = pfcode_count_pairs[:args.num_top_errors] + [("Other", n_others)]
    pfcode_frac_pairs = [(pfcd, float(ct)/n_passing) for pfcd, ct in pfcode_count_pairs]
    pfcode_probability_map = dict(pfcode_frac_pairs)
    return pfcode_probability_map

def get_populate_pfcode_probability_map(args):
    broken_lines = [l.strip().split()
        for l in open(PFCODE_COUNT_FNAME,"r").readlines()]
    pfcode_to_count = dict(
        (l[0], int(l[1]))
        for l in broken_lines)
    pfcode_to_prob = dict(
        (pfcode, float(count)/pfcode_to_count[PASSING_PFCODE])
        for pfcode, count in pfcode_to_count.items()
        if pfcode != PASSING_PFCODE
        )
    return pfcode_to_prob

# Create file that contains the number of occurrences of each pfcode
def count_pfcodes():
    query = """
    SELECT %(PFCODE_COLUMN)s, COUNT(*)
    FROM %(TEST_RESULT_TABLE)s
    GROUP BY %(PFCODE_COLUMN)s
    """ % {"PFCODE_COLUMN":PFCODE_COLUMN, "TEST_RESULT_TABLE":TEST_RESULT_TABLE}
    command = 'hive -e "' + query + '" >' + PFCODE_COUNT_FNAME
    print "Counting pfcode occurrences"
    start = time.time()
    os.system(command)
    stop = time.time()
    print "Took ", stop-start, " seconds"

# Create "target table" as a Hive table
def create_target_table():
    populate_pfcode_probability_map = get_populate_pfcode_probability_map(args)
    pfcodes_probabilities_str = " ".join(k+" "+str(v)
            for k, v in populate_pfcode_probability_map.items())
    query = """
    ADD FILE streamer.py;
    CREATE TABLE %(TARGET_TABLE_NAME)s AS
    SELECT TRANSFORM (%(PFCODE_COLUMN)s, %(TEST_RESULT_KEY_COLUMN)s)
    USING 'python streamer.py %(pfcodes_probabilities_str)s'
    AS (pfcode_grp, %(PFCODE_COLUMN)s, %(TEST_RESULT_KEY_COLUMN)s)
    FROM (
        SELECT *
        FROM %(TEST_RESULT_TABLE)s
        WHERE %(PFCODE_COLUMN)s IS NOT NULL
        ) foo
    """ % {"TARGET_TABLE_NAME":TARGET_TABLE_NAME, "PFCODE_COLUMN":PFCODE_COLUMN,
            "TEST_RESULT_KEY_COLUMN": TEST_RESULT_KEY_COLUMN, "pfcodes_probabilities_str": pfcodes_probabilities_str
    }
    command = 'hive -e "' + query + '"'
    start = time.time()
    os.system(command)
    stop = time.time()
    print "Took ", stop-start, " seconds"




# The CORR function in Hive is broken!  VERY broken!
# This function calculates the correlation in terms
# of moments of the distributions.  Yeah I know it's ugly.
# It also calculates the number of non-null failures there were
def get_corr_str(y):
    corr_template = "COVAR_POP(fail,%(feature)s) / (STDDEV_POP(fail)*STDDEV_POP(%(feature)s))"
    nonull_count_template = "SUM(IF(fail=1 and %(feature)s is not null, 1, 0))"
    total_template = corr_template + ", " + nonull_count_template
    if y in util.HIVE_KEYWORDS: ret = total_template % {"feature": "`" + y + "`"}
    else: ret = total_template % {"feature": y}
    return ret

def calculate_correlations(tab):
    columns_w_types = util.get_table_schema_w_types(tab)
    columns = [col for col, tp in columns_w_types]
    cors = [
        (get_corr_str(col)
        if (tp in ("float","int") and col not in util.HIVE_KEYWORDS)
        else "'NULL', 'NULL'"
        )
        for col, tp in columns_w_types]
    correlations_str = ", ".join(cors)
    params = {"PASSING_PFCODE": PASSING_PFCODE, "TARGET_TABLE_NAME": TARGET_TABLE_NAME,
        "TEST_RESULT_KEY_COLUMN": TEST_RESULT_KEY_COLUMN
    }
    query = """
    set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
    SELECT pfcode_grp, %(correlations_str)s
    FROM (
        SELECT
            pfcode_grp,
            IF(tar.pfcode = '%(PASSING_PFCODE)s', 0, 1) as fail,
            tab.*
        FROM (SELECT * FROM %(TARGET_TABLE_NAME)s) tar
        JOIN (SELECT * FROM %(table)s) tab
        ON TRIM(tar.%(TEST_RESULT_KEY_COLUMN)s) = TRIM(tab.%(TEST_RESULT_KEY_COLUMN)s)
    ) foo
    GROUP BY pfcode_grp, bootstrap_sample
    """ % params
    command = 'hive -e "' + query + '\n" > ' + tab + '.csv'
    print "Calculating correlations for table ", tab
    start = time.time()
    os.system(command)
    stop = time.time()
    print "Took ", stop-start, " seconds"


def format_top_features(args):
    # Get list of features, w other data
    all_results = []
    for tab in TABLES_TO_JOIN:
        print "processing table ", tab
        columns = [col for col, tp in util.get_table_schema_w_types(tab)]
        fname = "%s.csv" % tab
        for l in open(fname,'r').readlines():
            parts = l.strip().split("\t")
            pfcode = parts[0]
            cors_and_nonnull_counts = parts[1:]
            cors, nonnull_counts = parts[1::2], parts[2::2]
            cors = [util.str2float(x) for x in cors]
            nonnull_counts = [util.str2int(x) for x in nonnull_counts]
            new_results = [
                {
                "pfcode": pfcode,
                "table": tab,
                "feature": feature,
                "weight": weight,
                "n_nonnull_fails": n_nonnull_fails,
                }
                for feature, weight, n_nonnull_fails
                in zip(columns, cors, nonnull_fail_counts)
            ]
            all_results.extend([r for r in new_results if r["weight"] and r["n_nonnull_fails"]])
    all_results = sorted(all_results, key=lambda r: r["weight"])
    outfile = open(FEATURE_WEIGHTS_FILENAME, "w")
    schema = ["pfcode", "table", "feature", "weight", "n_nonnull_fails"]

    outfile.write("\t".join(schema) + "\n")
    for r in all_results:
        vals = [r[c] for c in schema]
        outfile.write("\t".join(vals) + "\n")

    outfile.close()

def main():
    count_pfcodes()
    create_target_table()
    for tab in tables_to_compare:
        calculate_correlations(tab)
    format_top_features()
