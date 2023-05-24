import json
import sys
from datetime import datetime

sys.path.append("..")
from app import generate_cross_service_data


def test_multi_service_multi_child():
    file = "flame_multi_service_multi_child.json"
    f = open(file)
    data = json.load(f)
    f.close()

    for item in data:
        item["timestamp"] = datetime.strptime(item["timestamp"], "%Y-%m-%d %H:%M:%S.%f")

    timesByPathByTree = generate_cross_service_data(data)

    out = json.dumps(timesByPathByTree, indent=4, default=str)
    fout = open("results/" + file, "w")
    fout.write(out)
    fout.close


test_multi_service_multi_child()
