from dataclasses import dataclass
from typing import Dict, List, Set
from google.protobuf import text_format
from datetime import datetime, timedelta
import json


# @dataclass
# class ExecutionPathInfo:
#     trace_id: str
#     root: str
#     span_ids: Set[str]
#     node_to_children: Dict[str, List[str]]


def get_trace_dicts(trace_data: Dict) -> List[Dict]:
    print(json.dumps(trace_data, indent=4))

    dicts = []

    for resource in trace_data["resourceSpans"]:
        for scope in resource["scopeSpans"]:
            spanIdToPath: Dict[str, str] = {}
            # incompleteExecutionPathsByTrace: Dict[str, any] = {}
            # Sort by start time so we always process classers before callees
            print("=======Unsorted========")
            print(json.dumps(scope["spans"], indent=4))
            scope["spans"].sort(
                key=lambda s: (
                    int(s["startTimeUnixNano"]),
                    -int(s["endTimeUnixNano"]),
                )
            )
            current_execution_path = {"#all_span_ids": set(), "#root": ""}
            all_execution_paths = []

            print("=======Sorted========")
            print(json.dumps(scope["spans"], indent=4))
            for span in scope["spans"]:
                dict = get_standard_trace_dict(span)

                func_id = dict["qualName"] + ":" + dict["file"]
                print(dict)

                parent = (
                    span["parentSpanId"]
                    if "parentSpanId" in span
                    else (span["parentId"] if "parentId" in span else None)
                )

                # if continuing from a previous incomplete execution path
                if parent and "parentFromOtherService" not in dict:
                    dict["parentSpanId"] = span["parentSpanId"]
                    print(json.dumps(spanIdToPath))
                    dict["path"] = spanIdToPath[dict["parentSpanId"]] + "," + func_id
                    parentName = spanIdToPath[dict["parentSpanId"]].split(",")[-1]
                    if parentName in current_execution_path:
                        if func_id not in current_execution_path[parentName]:
                            current_execution_path[parentName].append(func_id)
                    else:
                        current_execution_path[parentName] = [func_id]
                else:
                    print("got here")
                    print(current_execution_path)
                    if len(current_execution_path["#all_span_ids"]) >= 1:
                        print("merging")
                        all_execution_paths = merge_execution_paths(
                            all_execution_paths, current_execution_path
                        )
                        print(all_execution_paths)
                    current_execution_path = {
                        "#all_span_ids": set([dict["spanId"]]),
                        "#root": func_id,
                    }
                    dict["path"] = func_id

                current_execution_path["#all_span_ids"].add(dict["spanId"])
                spanIdToPath[dict["spanId"]] = dict["path"]

                dicts.append(dict)
            # all_execution_paths = []
            # print(incompleteExecutionPathsByTrace)
            # for _, executionPathInfo in incompleteExecutionPathsByTrace.items():
            #     print(executionPathInfo)
            #     all_execution_paths = merge_execution_paths(
            #         all_execution_paths, executionPathInfo
            #     )
            #     print(all_execution_paths)
            if len(current_execution_path["#all_span_ids"]) >= 1:
                all_execution_paths = merge_execution_paths(
                    all_execution_paths, current_execution_path
                )

            path_strings = []
            for execution_path in all_execution_paths:
                path_strings.append(
                    {
                        "all_span_ids": execution_path["#all_span_ids"],
                        "executionPathString": generate_execution_path_string(
                            execution_path["#root"], execution_path
                        ),
                    }
                )

            print(path_strings)

            for dict in dicts:
                for path in path_strings:
                    if dict["spanId"] in path["all_span_ids"]:
                        dict["executionPathString"] = path["executionPathString"]
    print(dicts)
    return dicts


# Given list of execution paths and new execution path, add new path if unique
def merge_execution_paths(all_execution_paths, current_execution_path) -> Dict:
    if len(all_execution_paths) == 0:
        all_execution_paths = [current_execution_path]
    matches = [
        execution_path_equal(path, current_execution_path)
        for path in all_execution_paths
    ]
    if not all(matches):
        all_execution_paths.append(current_execution_path)
    else:
        index = matches.index(True)
        all_execution_paths[index]["#all_span_ids"].update(
            current_execution_path["#all_span_ids"]
        )
    print("merge")
    print(all_execution_paths)

    return all_execution_paths


def execution_path_equal(execution_path1, execution_path2) -> bool:
    for name, children in execution_path2.items():
        if name == "#root":
            if children != execution_path1["#root"]:
                return False
        if name != "#all_span_ids":
            if name in execution_path1:
                print(name)
                if len(children) == len(execution_path1[name]):
                    for i in range(len(children)):
                        if children[i] != execution_path1[name][i]:
                            return False
                else:
                    return False
            else:
                return False
    return len(execution_path1) == len(execution_path2)


def generate_execution_path_string(curr: str, nodeChildren: Dict):
    execution_string = curr

    if curr in nodeChildren:
        execution_string += "("
        for i, child in enumerate(nodeChildren[curr]):
            execution_string += generate_execution_path_string(child, nodeChildren)
            if i != len(nodeChildren[curr]) - 1:
                execution_string += "|"
        execution_string += ")"
    return execution_string


def get_standard_trace_dict(trace: Dict):
    dict = {}
    dict["name"] = trace["name"]
    dict["spanId"] = trace["spanId"]
    dict["traceId"] = trace["traceId"]
    dict["responseTime"] = (
        int(trace["endTimeUnixNano"]) - int(trace["startTimeUnixNano"])
    ) / (10**6)
    dict["timestamp"] = datetime.utcfromtimestamp(
        float(trace["startTimeUnixNano"]) / 1e9
    )

    dict["args"] = []
    for attribute in trace["attributes"]:
        if attribute["key"] == "file":
            dict["file"] = attribute["value"]["stringValue"]
        elif attribute["key"] == "qualName":
            dict["qualName"] = attribute["value"]["stringValue"]
        elif attribute["key"] == "commit_id":
            dict["commit_id"] = attribute["value"]["stringValue"]
        elif attribute["key"] == "branch":
            dict["branch"] = attribute["value"]["stringValue"]
        elif attribute["key"] == "message":
            dict["commit_message"] = attribute["value"]["stringValue"]
        elif attribute["key"] == "parentFromOtherService":
            dict["parentFromOtherService"] = attribute["value"]["boolValue"]
            dict["parentSpanId"] = trace["parentSpanId"]
        elif attribute["key"] == "childInOtherService":
            dict["childInOtherService"] = attribute["value"]["boolValue"]
        else:
            # any other attribute is an arg
            dict["args"].append(attribute)

    return dict
