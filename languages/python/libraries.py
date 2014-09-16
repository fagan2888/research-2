
import re

ages = [x.split()[1]
  for x in re.findall("Age: [0-9]*", text)]

