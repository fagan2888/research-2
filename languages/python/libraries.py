"""
To know
- findall
- match  # start of string only
- search  # iterator of MatchObjects
- split
"""
import re
ages = [x.split()[1]
  for x in re.findall("Age: [0-9]+", text)]  # + means at least one digit.  * could have 0
alpha_numeric = re.compile("[0-9A-Za-z]+")
four_alphanum = re.compile("[0-9A-Za-z]{4}")



"""
To know
- ArgumentParser
- add_argument
- parse_args
"""
import argparse
def parse_command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", dest="month")
    parser.add_argument("--date", dest="date")
    parser.add_argument("--target_slidersn_table", dest="target_slidersn_table", type=str)
    parser.add_argument("--num_top_errors", dest="num_top_errors", type=int, default=20)
    args = parser.parse_args()
    print "month", args.month
    return args


"""
To know
- call
- check_output
"""
import subprocess as sp
sp.call(["mkdir", "foo"])
directory_contents = sp.check_output(["ls", "."])



"""
To know
- groupby
"""
import itertools
parse_unique_test_id = lambda x: x.split()[0]
for key, line_iter in itertools.groupby(sys.stdin, parse_unique_test_id):
    print "Lines for key ", key
    for slider_failed, l in zip(slider_failures, broken_lines):
        print l

"""
os
- getcwd
- chdir
- listdir
"""
import os
cur_dir = os.getcwd()
os.chdir("/home/field")
directory_contents = os.listdir(".")
my_process = os.popen("")

"""
os.path
"""
os.path.isfile("myfile.py")
os.path.isdir("my_directory")
os.path.exists("my_maybe_there_file.txt")

"""
json
- loads
- dumps
"""

