from typing import Dict
from flask import Flask, request, json, Response
from google.protobuf import text_format
from datetime import datetime, timedelta
from opentelemetry.proto.trace.v1 import trace_pb2
from google.protobuf.json_format import MessageToDict
import json
import pymongo
from flask_cors import cross_origin, CORS
import re
from pymongo_get_database import get_database
from functools import reduce

from trace_processor import get_trace_dicts

db = get_database()

PORT = 8000


app = Flask(__name__)
CORS(app)


@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        res = Response()
        res.headers["X-Content-Type-Options"] = "*"
        return res


# @app.after_request
# def add_header(res):
#     if res.method != "OPTIONS":
#         res.headers['Access-Control-Allow-Origin'] = '*'
#         return res


@app.route("/get_all_execution_paths", methods=["POST"])
def get_all_execution_paths_accross_services():
    data = json.loads(request.data)
    # timesByPathByTree = get_children_for_root(data["rootPath"])

    # print(rootId)
    rootId = data["rootPath"]
    collection = db["OtelBackend"]["Traces"]
    regex_string = re.compile(f"([^.]+)?{re.escape(rootId)}([^.]+)?")
    # Match anything
    trace_ids = list(
        collection.find(
            {"executionPathString": {"$regex": regex_string}}, projection={"_id": False}
        ).distinct("traceId")
    )

    # Dict[traceId, Set()]
    traceIdToSpanIds = {}
    # Dict[spanId, data]
    spanIdToData = {}
    # Dict[parentSpanId, List[childSpanId]]
    remoteParentsToChildren = {}

    traces_cursor = collection.find(
        {"traceId": {"$in": trace_ids}}, projection={"_id": False}
    )

    ### First pass over traces, populate data structures for next section
    for trace in traces_cursor:
        spanIdToData[trace["spanId"]] = trace

        if "parentFromOtherService" in trace:
            if trace["parentSpanId"] in remoteParentsToChildren:
                remoteParentsToChildren[trace["parentSpanId"]].append(trace["spanId"])
            else:
                remoteParentsToChildren[trace["parentSpanId"]] = [trace["spanId"]]
        elif "childInOtherService" in trace:
            if trace["spanId"] not in remoteParentsToChildren:
                remoteParentsToChildren[trace["spanId"]] = []

        if trace["traceId"] in traceIdToSpanIds:
            traceIdToSpanIds[trace["traceId"]].add(trace["spanId"])
        else:
            traceIdToSpanIds[trace["traceId"]] = set([trace["spanId"]])

    ### Resolve links accross services
    for parentId, childIds in remoteParentsToChildren.items():
        parentData = spanIdToData[parentId]
        oldExecutionPath = parentData["executionPathString"]
        funcId = parentData["qualName"] + ":" + parentData["file"]

        toReplace = funcId
        alreadyHadChildren = False
        # If path in parent service already has children
        if funcId + "(" in oldExecutionPath:
            toReplace += "("
            alreadyHadChildren = True

        additionalExecutionPath = ""
        for i, id in enumerate(childIds):
            additionalExecutionPath += spanIdToData[id]["executionPathString"]
            if alreadyHadChildren or i != len(childIds) - 1:
                additionalExecutionPath += "|"

        if not alreadyHadChildren:
            additionalExecutionPath = "(" + additionalExecutionPath + ")"

        # combined execution path for trace
        newExecutionPath = oldExecutionPath.replace(
            toReplace, toReplace + additionalExecutionPath
        )

        # Update data from parent service
        for id in traceIdToSpanIds[parentData["traceId"]]:
            data = spanIdToData[id]
            if id in childIds:
                data["path"] = parentData["path"] + "," + data["path"]
                data["executionPathString"] = newExecutionPath

            elif data["executionPathString"] == oldExecutionPath:
                data["executionPathString"] = newExecutionPath

    timesByPathByTree = populate_times_by_path_by_tree(spanIdToData.values())
    print(json.dumps(timesByPathByTree, indent=4, default=str))

    return {"timesByPathByTree": timesByPathByTree}


@app.route("/get_trace_trees", methods=["POST"])
@cross_origin()
def get_trace_trees():
    data = json.loads(request.data)
    timesByPathByTree = get_children_for_root(data["rootPath"])
    return {"timesByPathByTree": timesByPathByTree}


def get_children_for_root(rootId: str) -> Dict:
    print(rootId)
    collection = db["OtelBackend"]["Traces"]
    regex_string = re.compile(f"(,)?{re.escape(rootId)}([^.]+)?")
    traces_cursor = collection.find(
        {"path": {"$regex": regex_string}}, projection={"_id": False}
    )
    return populate_times_by_path_by_tree(traces_cursor)


def populate_times_by_path_by_tree(traces_iterable):
    timesByPathByTree = {}
    for trace in traces_iterable:
        executionPathString = trace["executionPathString"]
        if executionPathString in timesByPathByTree:
            timesByPath = timesByPathByTree[executionPathString]
            if trace["path"] in timesByPath:
                timesByPath[trace["path"]].append(trace)
            else:
                timesByPath[trace["path"]] = [trace]
        else:
            timesByPathByTree[executionPathString] = {}
            timesByPathByTree[executionPathString][trace["path"]] = [trace]
    return timesByPathByTree


# Change to GET - query parameters?
@app.route("/get_mean_response_time", methods=["POST"])
@cross_origin()
def get_mean_response_time():
    data = json.loads(request.data)
    traces_cursor = query_traces(
        qualName=data["qualName"], filePath=data["filePath"], prevDays=data["prevDays"]
    )
    total_response_time = 0
    no_traces = 0
    for trace in traces_cursor:
        total_response_time += trace["responseTime"]
        no_traces += 1

    if no_traces > 0:
        return {"meanResponseTime": total_response_time / no_traces}, 200
    return {"meanResponseTime": 0}, 200


@app.route("/get_file_overview", methods=["POST"])
@cross_origin()
def get_file_overview():
    data = json.loads(request.data)
    traces_cursor = query_traces(filePath=data["filePath"], prevDays=data["prevDays"])
    totals = {}
    counts = {}

    for trace in traces_cursor:
        qualName = trace["qualName"]
        if qualName not in totals:
            totals[qualName] = 0
            counts[qualName] = 0
        totals[qualName] += trace["responseTime"]
        counts[qualName] += 1

    means = {key: totals[key] / counts[key] for key in totals.keys()}
    return means


@app.route("/get_all_for_function", methods=["POST"])
@cross_origin()
def get_all_for_functon():
    data = json.loads(request.data)
    traces_cursor = query_traces(
        qualName=data["qualName"], filePath=data["filePath"], prevDays=data["prevDays"]
    )
    return list(traces_cursor)


def query_traces(qualName=None, filePath=None, prevDays=7) -> pymongo.cursor:
    # TODO include file path in call
    query_date_start = datetime.now() - timedelta(days=prevDays)

    query = {"timestamp": {"$gte": query_date_start}}
    if qualName is not None:
        query["qualName"] = qualName
    if filePath is not None:
        matchString = re.escape(filePath)
        query["file"] = {"$regex": f".*{matchString}"}
    collection = db["OtelBackend"]["Traces"]

    return collection.find(query, projection={"_id": False})


@app.route("/v1/traces", methods=["POST"])
def traces():
    trace = trace_pb2.TracesData()
    trace.ParseFromString(request.data)

    trace_dicts = get_trace_dicts(MessageToDict(trace))

    collection = db["OtelBackend"]["Traces"]
    collection.insert_many(trace_dicts)

    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
