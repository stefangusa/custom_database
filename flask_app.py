import json
import random
import time

from boto3.session import Session
from collections import Counter
from flask import Flask, request
import hashlib


app = Flask(__name__)

SUCCESS = 200
BAD_REQUEST = 400
NOT_FOUND = 404
CONFLICT = 409
INTERNAL_SERVER_ERROR = 503

OPERATIONS = ['select', 'insert', 'update', 'delete']
THRESHOLD = 9

NODE_NO = None
NO_NODES = 3
TOPIC_ARN = None

session = Session(aws_access_key_id='',
                  aws_secret_access_key='')
sns = None
sqs = None


def parse_query(query):
    operation, raw_attr = query.split(' ', 1)
    if operation.lower() not in OPERATIONS:
        return 'Supported operations: "SELECT", "INSERT", "UPDATE", "DELETE', BAD_REQUEST

    raw_key_values = raw_attr.replace(' ', '').split(',')
    key_values = [raw_key_value.split(':') for raw_key_value in raw_key_values]
    if any(len(key_value) != 2 for key_value in key_values):
        return 'Attributes should be in "pk:value,key1:value1,..." form', BAD_REQUEST

    attributes = [tuple(key_value) for key_value in key_values]
    if any(not key or not value for (key, value) in attributes):
        return 'Attributes should be in "pk:value,key1:value1,..." form', BAD_REQUEST

    return operation, attributes


@app.route('/replace/<node_no>')
def replace(node_no):
    destination_list = '[' + ', '.join(['\"' + str(node) + '\"' for node in range(NO_NODES)]) + ']'

    sns.publish(
        Message=json.dumps({'default': json.dumps({'replace': node_no})}),
        MessageStructure='json',
        MessageAttributes={
            'destination': {
                'DataType': 'String.Array',
                'StringValue': destination_list
            }
        }
    )

    return "Replace in progress", SUCCESS


@app.route('/scale/<no_nodes>')
def scale(no_nodes):
    global NO_NODES

    destination_list = '[' + ', '.join(['\"' + str(node) + '\"' for node in range(max(int(no_nodes), NO_NODES))]) + ']'
    NO_NODES = int(no_nodes)

    sns.publish(
        Message=json.dumps({'default': json.dumps({'scale': str(NO_NODES)})}),
        MessageStructure='json',
        MessageAttributes={
            'destination': {
                'DataType': 'String.Array',
                'StringValue': destination_list
            }
        }
    )

    return "Scaling in progress", SUCCESS


@app.route('/', methods=["POST"])
def operate():
    query = request.form.get('query')
    if not query:
        return 'Field "query" is required in the request.', BAD_REQUEST

    operation, attributes = parse_query(query)
    if attributes == BAD_REQUEST:
        return operation, attributes
    (pk, pk_value) = attributes[0]
    destination = int(hashlib.sha256((pk + pk_value).encode('utf-8')).hexdigest(), 16) % NO_NODES
    message = {
        'operation': operation,
        'attributes': attributes,
        'id': random.getrandbits(32),
        'source': NODE_NO,
        'base_destination': str(destination)
    }

    sns.publish(
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json',
        MessageAttributes={
            'destination': {
                'DataType': 'String.Array',
                'StringValue': '["{0}", "{1}", "{2}"]'.format(
                    str((destination - 1) % NO_NODES), str(destination), str((destination + 1) % NO_NODES)
                )
            }
        }
    )

    time.sleep(5)
    num_messages = 3
    responses = []

    while num_messages <= THRESHOLD:
        raw_responses = sqs.receive_messages(MaxNumberOfMessages=num_messages)
        for raw_response in raw_responses:
            response = json.loads(json.loads(raw_response.body)['Message'])

            if response['id'] != message['id']:
                continue

            responses.append(response['response'])
            raw_response.delete()

            if len(responses) == 3:
                break

        if len(responses) == 3:
            break

        num_messages = num_messages + 3
        time.sleep(5)

    if len(responses) < 2 or len(set(responses)) in [0, 3] or (len(responses) == 2 and len(set(responses)) == 2):
        return "Internal Server Error", INTERNAL_SERVER_ERROR

    response = Counter(responses).most_common(1)[0][0]
    if response == 'Error' and operation == 'insert':
        return 'Item already exists.', CONFLICT
    elif response == 'Error':
        return 'Item could not be found.', NOT_FOUND
    return response, SUCCESS


def run_flask(node_no, topic_arn):
    global NODE_NO, TOPIC_ARN, sns, sqs
    NODE_NO = node_no
    TOPIC_ARN = topic_arn

    sns = session.resource('sns', region_name='eu-west-3') \
        .Topic(TOPIC_ARN)
    sqs = session.resource('sqs', region_name='eu-west-3') \
        .Queue("https://sqs.eu-west-3.amazonaws.com/565215661342/Node{}_Responses".format(NODE_NO))

    app.run(port=80, use_reloader=False, debug=True)