# Import the necessary libraries
import json
import boto3
import pymongo
import certifi

ca = certifi.where()

# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb+srv://project:projectphoenix@cluster1.opa1p0r.mongodb.net/?retryWrites=true&w=majority", tlsCAFile=ca)
db = client['vectorSearch']
collection = db['books']

newline, bold, unbold = '\n', '\033[1m', '\033[0m'
endpoint_name = 'jumpstart-dft-hf-textembedding-all-minilm-l6-v2'


def query_endpoint_with_json_payload(encoded_json):
    client = boto3.client('runtime.sagemaker')
    response = client.invoke_endpoint(EndpointName=endpoint_name, ContentType='application/json', Body=encoded_json)
    return response


def parse_response_multiple_texts(query_response):
    model_predictions = json.loads(query_response['Body'].read())
    embeddings = model_predictions['embedding']
    return embeddings


payload = {"text_inputs": ["mythology creatures"]}
query_response = query_endpoint_with_json_payload(json.dumps(payload).encode('utf-8'))
embeddings = parse_response_multiple_texts(query_response)

print("embeddings:" + str(embeddings[0]))

response = db.books.aggregate([
  {
    "$search": {
      "index": "vector-index",
      "knnBeta": {
        "vector": embeddings[0],
        "path": "egVector",
        "k": 5
     }
    }
  },
  {
    "$project": {
      "title": 1,
      "synopsis": 1
    }
  },
  { "$limit": 1}
])

for item in response:
        print({"page_content":item['synopsis'], "metadata":{"title":item['title']}})






