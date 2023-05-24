import json
import sys

sys.path.append("..")
from trace_processor import get_trace_dicts


def test_multi_child():
    file = "multi_child_otel_span.json"

    f = open(file)
    data = json.load(f)
    f.close()
    dicts = get_trace_dicts(data)
    out = json.dumps(dicts, indent=4, default=str)

    fout = open("results/" + file, "w")
    fout.write(out)
    fout.close


test_multi_child()
