import json

n=0
with open("./straitstimes.jsonl") as f:
    for i, line in enumerate(f):
        item=json.loads(line)
        item["byline"]=item["byline"].split(",")[0]
        print(item["byline"], flush=True)
    print("{}".format(i), flush=True)

