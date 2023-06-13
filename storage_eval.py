from pymongo_get_database import get_database

# import sys


def demo_traces(doclist, db):
    collection = db["OtelBackend"]["TracesDemo"]
    collection.insert_many(doclist)


def noraml_storage(doclist):
    print(f"{len(doclist)} docs")
    total = 0
    for doc in doclist:
        total += sys.getsizeof(doc)

    print(total)
    print(total / len(doclist))


def storage_without_path(doclist, db):
    print(doclist[0])
    for doc in doclist:
        del doc["path"]
        del doc["executionPathString"]

    collection = db["OtelBackend"]["DemoWithoutPath"]
    collection.insert_many(doclist)


def execution_path_opt(doclist, db):
    pathIDToID = {}
    pathCollection = db["OtelBackend"]["DemoPathOptPaths"]
    for doc in doclist:
        pathID = doc["path"] + "+" + doc["executionPathString"]
        id = None
        if pathID not in pathIDToID:
            id = pathCollection.insert_one(
                {"path": doc["path"], "executionPathString": doc["executionPathString"]}
            ).inserted_id
            pathIDToID[pathID] = id
        else:
            id = pathIDToID[pathID]
        del doc["path"]
        del doc["executionPathString"]
        doc["pathId"] = id

    collection = db["OtelBackend"]["DemoPathOpt"]
    collection.insert_many(doclist)


# def improved_storage(doclist):
#     print(f"{len(doclist)} docs")
#     fakeObjectId = doclist[0]["_id"]

#     uniqueCommitIdToDoc = set()
#     uniquePathStrToDoc = set()

#     for doc in doclist:
#         if doc["commit_id"] not in uniqueCommitIdsToDoc:


db = get_database()
collection = db["OtelBackend"]["TracesDemo"]
# collection.createIndex({})
doclist = list(collection.find({"name": {"$nin": ["child1"]}}))

print(len(list(doclist)))


# trace_ids = collection.distinct("traceId")
# print(len(trace_ids))
# print(list(collection.distict("name")))
# execution_path_opt(doclist, db)
# # demo_traces(doclist, db)
# storage_without_path(doclist, db)
# # print("done")
# execution_path_opt(doclist, db)
# # # noraml_storage(doclist, db)
# # storage_without_path(doclist, db)
