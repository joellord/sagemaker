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


# payload = {"text_inputs": [text1, text2, text3]}
# query_response = query_endpoint_with_json_payload(json.dumps(payload).encode('utf-8'))
# embeddings = parse_response_multiple_texts(query_response)

# Get all documents in the collection
documents = collection.find()

print("started processing...")
i = 0
# Loop over all documents
for document in documents:

    i += 1
    # print("title: " + document['title'])
    # print(document['synopsis'])
    query = {'_id': document['_id']}

    if 'synopsis' in document and 'egVector' not in document:
        payload = {"text_inputs": [document['synopsis']]}
        query_response = query_endpoint_with_json_payload(json.dumps(payload).encode('utf-8'))
        embeddings = parse_response_multiple_texts(query_response)
        # print("embeddings: " + str(embeddings[0]))

        # update the document
        update = {'$set': {'egVector':  embeddings[0]}}
        collection.update_one(query, update)

    if i % 50 == 0:
        print("processed: " + str(i) + " records")

    # if i > 200:
    #     break

print("finished processing: " + str(i) + " records")
