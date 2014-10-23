"""

"""

import util


SLIDERSN_SELECTION_TEMPLATE = """
time hive -e "
ADD FILE streamer.py;

DROP TABLE %(TARGET_SLIDERSN_TABLE)s;

CREATE EXTERNAL TABLE %(TARGET_SLIDERSN_TABLE)s (
    enddate string,
    pfcode_grp string,
    bootstrap_sample int,
    pfcode string,
    hddsn string,
    slidersn string,
    mtype string,
    pfsubcode string
)
LOCATION '/user/field/%(TARGET_SLIDERSN_TABLE)s'
;

INSERT OVERWRITE TABLE %(TARGET_SLIDERSN_TABLE)s
SELECT TRANSFORM (enddate, pfcode, hddsn, slidersn, mtype, pfsubcode)
USING 'python streamer.py %(num_bootstrap_samples)s %(pfcodes_probabilities_str)s'
AS (enddate, pfcode_grp, bootstrap_sample, pfcode, hddsn, slidersn, mtype, pfsubcode)
FROM (
    SELECT *
    FROM %(ASSOCIATION_TABLE)s
    WHERE
        year=%(year)s AND
        month=%(month)s AND
        --SUBSTR(enddate,0,8)=date AND
        slidersn IS NOT NULL) asso
"
"""

HIVE_QUERY_TEMPLATE = """
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SELECT the_enddate, pfcode_grp, bootstrap_sample, %(correlations_str)s
FROM (
    SELECT
        pfcode_grp,
        IF(tar.pfcode = '%(PASSING_PFCODE)s', 0, 1) as fail,
        SUBSTR(tar.enddate,0,8) as the_enddate,
        tar.bootstrap_sample as bootstrap_sample,
        tab.*
    FROM (SELECT * FROM %(TARGET_SLIDERSN_TABLE)s) tar
    JOIN (SELECT * FROM %(table)s) tab
    ON TRIM(tar.slidersn) = TRIM(tab.%(slidersn_colname)s)
) foo
GROUP BY the_enddate, pfcode_grp, bootstrap_sample
"""

# Used in sampling passing drives to compare against failing ones.
# Returns map from pfcode -> probability that a random PASSING drive should be included
#   in the comparison for that pfcode.
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

# The CORR function in Hive is broken!  VERY broken!
# This function calculates the correlation in terms
# of moments of the distributions.  Yeah I know it's ugly.
# It also calculates the number of non-null failures there were
def get_corr_str(y):
    corr_template = "COVAR_POP(fail,%(feature)s) / (STDDEV_POP(fail)*STDDEV_POP(%(feature)s))"
    nonull_count_template = "SUM(IF(fail=1 and %(feature)s is not null, 1, 0))"
    total_template = corr_template + ", " + nonull_count_template
    if y in util.HIVE_KEYWORDS:
        ret = total_template % {"feature": "`" + y + "`"}
    else:
        ret = total_template % {"feature": y}
    return ret

# Creates query for a given table and writes it to a file.
# Returns shell command that will run that query.
# Note: queries are written to file because the long ones have too many
# characters to pass in to hive -e
def write_query(tab, errors, target_slidersn_table):
    columns_w_types = util.get_table_schema_w_types(tab)
    columns = [col for col, tp in columns_w_types]
    cors = [
        (get_corr_str(col)
        if (tp in ("float","int") and col not in util.HIVE_KEYWORDS)
        else "'NULL', 'NULL'"
        )
        for col, tp in columns_w_types]
    params = {
        "table": tab,
        "pfcodes": " ".join(errors),
        "correlations_str": ", ".join(cors),
        "TARGET_SLIDERSN_TABLE": target_slidersn_table,
        "PASSING_PFCODE": util.PASSING_PFCODE
    }
    if "slidersn" in columns: params["slidersn_colname"] = "slidersn"
    elif "sliderid" in columns: params["slidersn_colname"] = "sliderid"
    else: return None
    query = HIVE_QUERY_TEMPLATE % params
    open(tab+".hql", "w").write(query)
    command = '\n\ntime hive -f ' + tab + '.hql > ' + ('%(table)s_w_nonnull_counts.csv' % params)

    return command

def main():
    args = util.parse_command_line_args()
    parts = []
    
    # Query to create "target" table.  Included bootstrapping, etc.
    populate_pfcode_probability_map = get_populate_pfcode_probability_map(args)
    pfcodes_probabilities_str = " ".join(k+" "+str(v)
            for k, v in populate_pfcode_probability_map.items())
    top_errors = util.get_top_errors(args)
    slidersn_select_query = SLIDERSN_SELECTION_TEMPLATE % {
        "TARGET_SLIDERSN_TABLE": args.target_slidersn_table,
        "ASSOCIATION_TABLE": args.association_table,
        "num_bootstrap_samples": args.num_bootstrap_samples,
        "pfcodes_probabilities_str": pfcodes_probabilities_str,
        "month":args.month,
        "year":args.year
    }
    parts.append(slidersn_select_query)
    
    # For each parametric table, query to join it against the target table
    # and calculate correlations
    tables = util.get_tables_to_join()
    for tab in tables:
        query = write_query(tab, top_errors, args.target_slidersn_table)
        if query: parts.append(query)

    open(util.FEATURE_SELECTOR_SCRIPT_FNAME, "w").write("\n".join(parts))
            

if __name__ == "__main__":
    main()


