"""
This script does all sampling.  It is streamed (stdin) the info for different sliders,
including their ultimate pfcode.  It print (to stdout) which sliders should be selected
in which sample.

Calling this script (5 bootstrap samples, etc)
$ python streamer.py 5 4801 0.00389174814937 4301 0.0040411434718

"""

import sys, random
from itertools import groupby

PASSING_PFCODE = "0000"
DELIM = "\t"
OTHER_PFCODE = "Other"

INPUT_SCHEMA =  [              "pfcode", "hddsn", "slidersn", "mtype", "pfsubcode"]
OUTPUT_SCHEMA = ["pfcode_grp", "pfcode", "hddsn", "slidersn", "mtype", "pfsubcode"]

num_bootstrap_samples = None

# Map from pfcode to the fraction of all passing sliders
# that should be compared against it.
pfcode_probability_map = None

def populate_params_from_command_line():
    global num_bootstrap_samples
    num_bootstrap_samples = int(sys.argv[1])
    global pfcode_probability_map
    pfcodes = sys.argv[2::2]
    probs = [float(x) for x in sys.argv[3::2]]
    pfcode_probability_map = dict(zip(pfcodes, probs))
    pfcode_probability_map[PASSING_PFCODE] = max(pfcode_probability_map.values())

def pfsubcode_to_bools(sc, num_sliders):
    return [(char=="1") for char in
        bin(int(sc, 16))[2:].rjust(num_sliders,"0")]

def parse_unique_test_id(line):
    record = dict(zip(INPUT_SCHEMA, line.strip().split()))
    return (record["enddate"], record["hddsn"])  # enddate accounts for re-testing

def process_failing_record(pfcd, record):
    if pfcode_probability_map.has_key(pfcd): record["pfcode_grp"] = pfcd
    else: record["pfcode_grp"] = OTHER_PFCODE
    for sample_num in range(num_bootstrap_samples):
        print sample_num
        record["bootstrap_sample"] = str(sample_num)
        print DELIM.join([record[c] for c in OUTPUT_SCHEMA])

def process_passing_record(pfcd, record):
    global pfcode_probability_map
    pfcodes_to_grp = pfcode_probability_map.keys() + [OTHER_PFCODE]
    for pc in pfcodes_to_grp:
        passing_sample_fraction = pfcode_probability_map[pfcd]
        record["pfcode_grp"] = pc
        for sample_num in range(num_bootstrap_samples):
            if random.uniform(0,1) > passing_sample_fraction: continue
            record["bootstrap_sample"] = str(sample_num)
            print DELIM.join([record[c] for c in OUTPUT_SCHEMA])

def main():
    populate_params_from_command_line()
    for key, line_iter in groupby(sys.stdin, parse_unique_test_id):
        # Process all sliders in a given HDD  (for a given time it's tested)
        broken_lines = [l.strip("\n").split(DELIM) for l in line_iter]
        line_lengths = [len(l) for l in broken_lines]
        if any(ln!=len(INPUT_SCHEMA) for ln in line_lengths):
            # If any lines formatted wrong, skip this HDD
            continue
        first_record = dict(zip(INPUT_SCHEMA, broken_lines[0]))
        pfsubcode = first_record["pfsubcode"]
        if len(pfsubcode.strip())==0:
            # If pfsubcode wrong format, skip HDD
            continue
        slider_failures = pfsubcode_to_bools(pfsubcode, len(broken_lines))
        for slider_failed, l in zip(slider_failures, broken_lines):
            record = dict(zip(INPUT_SCHEMA, l))
            pfcd = (record["pfcode"] if slider_failed else PASSING_PFCODE)
            record["pfcode"] = pfcd
            record["enddate"] = record["enddate"][:8]  # trim to YYYYMMDD
            if pfcd == PASSING_PFCODE: process_passing_record(pfcd, record)
            else: process_failing_record(pfcd, record)

main()
