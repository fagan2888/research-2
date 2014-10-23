import sys
import util
import math


def isNaN(num):
    return num != num

def str_to_corr(x):
    try:
        res = float(x)
        if isNaN(res): return util.NULL_WEIGHT
        else: return res
    except: return util.NULL_WEIGHT

def format_top_detractors(args):
    top_errors = set(util.get_top_errors(args))
    # Group by date
    records_by_date = {}
    for l in open(args.error_count_fname,'r').readlines():
        date_YYYYMMDD, pfcode, ct = l.split()
        r = {"date": date_YYYYMMDD, "pfcode": pfcode, "count": int(ct)}
        try: records_by_date[date_YYYYMMDD].append(r)
        except: records_by_date[date_YYYYMMDD] = [r]
    # Calculate total failures per day and num OTHER pfcodes
    for date, records in records_by_date.items():
        n_total_hdds = sum(r["count"] for r in records)
        n_others = sum(r["count"] for r in records
            if r["pfcode"] not in top_errors and r["pfcode"] != util.PASSING_PFCODE
            )
        top_error_records = [r for r in records if r["pfcode"] in top_errors]
        other_record = {"date":date, "pfcode":util.OTHER_PFCODE, "n_total_hdds":n_total_hdds, "count":n_others}
        if util.INCLUDE_PFCODE_OTHER: recs = top_error_records + [other_record]
        else: recs = top_error_records
        for r in recs:
            r["n_total_hdds"] = str(n_total_hdds)
            r["product"] = util.PRODUCT
            r["fraction"] = str(float(r["count"]) / n_total_hdds)
            r["count"] = str(r["count"])
        records_by_date[date] = recs
    # Add zero counts for days w no data
    pfcodes_to_include = list(top_errors)
    if util.INCLUDE_PFCODE_OTHER: pfcodes_to_include.append(util.OTHER_PFCODE)
    for date, records in records_by_date.items():
        pfcodes_already_included = set(r["pfcode"] for r in records)
        for pfcd in pfcodes_to_include:
            if pfcd in pfcodes_already_included: continue
            else:
                new_rec = records[0].copy()
                new_rec.update({"pfcode":pfcd, "count":"0", "fraction":"0.0"})
                records.append(new_rec)
    # Produce output
    results = reduce(lambda x, y: x+y, records_by_date.values())
    split_output_lines = [[record[c] for c in util.TOP_DETRACTORS_SCHEMA] for record in results]
    output_lines = [util.DELIM.join(l) for l in split_output_lines]
    open(util.TOP_DETRACTORS_FNAME,"w").write("\n".join(output_lines))


def aggregate_cors(ws):
    if len(ws) < 4: return 0
    return abs(float(sum(ws)) / len(ws))

def format_top_features(args):
    # Get list of features, w other data
    tables = util.get_tables_to_join()
    all_results = []
    for tab in tables:
        print "processing table ", tab
        try:
            columns = [col for col, tp in util.get_table_schema_w_types(tab)]
            fname = "%s_w_nonnull_counts.csv" % tab
            for l in open(fname,'r').readlines():
                parts = l.strip().split(util.DELIM)
                date, pfcode, bootstrap_sample = parts[0], parts[1], parts[2]
                cors_and_nonnull_counts = parts[3:]
                cors = [str_to_corr(x) for x in cors_and_nonnull_counts[0::2]]
                nonnull_fail_counts = [
                    (int(x) if x.isdigit() else 0)
                    for x in cors_and_nonnull_counts[1::2]]
                new_results = [
                        {"date": date, "product": util.PRODUCT,
                        "metric": util.FEATURE_WEIGHT_METRIC,
                        "bootstrap_sample": bootstrap_sample,
                        "pfcode": pfcode,
                        "table": tab,
                        "feature": feature,
                        "weight": weight,
                        "n_nonnull_fails": n_nonnull_fails,
                        }
                        for feature, weight, n_nonnull_fails
                        in zip(columns, cors, nonnull_fail_counts)
                    ]
                new_valid_results = [r for r in new_results if
                    r["weight"] != util.NULL_WEIGHT and
                    r["weight"]<util.MAX_WEIGHT and
                    r["n_nonnull_fails"]>=util.MIN_N_NONNULL_DATAPOINTS
                    ]
                del new_results
                all_results.extend(new_valid_results)
        except:
            print "failed"
            continue
    
    valid_results = all_results
    
    print "len valid", len(valid_results)
    if not util.INCLUDE_PFCODE_OTHER:
        valid_results = [r for r in valid_results if r["pfcode"] != util.OTHER_PFCODE]
    
    # Aggregate across each bootstrap sample to get aggregate weight
    by_pfcode_date_table_feature = util.groupby(valid_results, ["pfcode", "date", "table", "feature"])
    agg_results = {}
    for k, recs in by_pfcode_date_table_feature.items():
        agg_weight = aggregate_cors([r["weight"] for r in recs])
        first_rec = recs[0].copy()
        first_rec["weight"] = agg_weight
        agg_results[k] = first_rec
    
    # Group features by date/pfcode and take top 20
    by_pfcode_date = util.groupby(agg_results.values(), ["pfcode", "date"])
    output_results = []
    top_errors = set(util.get_top_errors(args))
    for key, lst in by_pfcode_date.items():
        pf, date = key
        if pf not in top_errors: continue
        lst.sort(key = lambda r: str_to_corr(r["weight"]), reverse=True)
        ln = min(len(lst), args.num_top_features)
        output_results.extend(lst[:ln])
    split_output_lines = [
        [str(record[c]) for c in util.TOP_FEATURES_SCHEMA]
        for record in output_results]
    output_lines = [util.DELIM.join(l) for l in split_output_lines]
    open(util.TOP_FEATURES_FNAME,"w").write("\n".join(output_lines))


def main():
    args = util.parse_command_line_args()

    format_top_detractors(args)
    format_top_features(args)

main()


