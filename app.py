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


@app.route("/get_trace_trees", methods=["POST"])
@cross_origin()
def get_trace_trees():
    data = json.loads(request.data)
    timesByPathByTree = get_children_for_root(data["rootPath"])
    return {"timesByPathByTree": timesByPathByTree}


def get_children_for_root(rootId: str) -> Dict:
    print(rootId)
    collection = db["OtelBackend"]["Traces"]
    regex_string = re.compile(f"(,)?{rootId}([^.]+)?")
    traces_cursor = collection.find(
        {"path": {"$regex": regex_string}}, projection={"_id": False}
    )
    timesByPathByTree = {}
    for trace in traces_cursor:
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
        print(timesByPathByTree)
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
