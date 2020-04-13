import json
import logging
import os
import random
import time

from boto3.session import Session
from collections import Counter
from flask import Flask, request
from flask.logging import default_handler
import hashlib


app = Flask(__name__)

LOG_FILE = '/var/log/flask_app.log'
os.environ['WERKZEUG_RUN_MAIN'] = 'true'

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
    try:
        if int(node_no) >= NO_NODES:
            raise ValueError
    except ValueError:
        return "Node number should be an integer between 0 and {}".format(NO_NODES), BAD_REQUEST

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
    try:
        if int(no_nodes) < 3 or int(no_nodes) > 10:
            raise ValueError
    except ValueError:
        return "No. nodes must be an integer between 3 and 10.", BAD_REQUEST

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


@app.route('/check')
def working():
    return "Working!", SUCCESS


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


def run_flask(node_no, topic_arn, log_level):
    global NODE_NO, TOPIC_ARN, sns, sqs
    NODE_NO = node_no
    TOPIC_ARN = topic_arn

    sns = session.resource('sns', region_name='eu-west-3') \
        .Topic(TOPIC_ARN)
    sqs = session.resource('sqs', region_name='eu-west-3') \
        .Queue("https://sqs.eu-west-3.amazonaws.com/565215661342/Node{}_Responses".format(NODE_NO))

    logging.basicConfig(filename=LOG_FILE)
    handler = logging.FileHandler(LOG_FILE)
    handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)
    app.logger.removeHandler(default_handler)

    if log_level == logging.DEBUG:
        debug = True
    else:
        debug = False

    app.run(host='0.0.0.0', port=80, use_reloader=False, debug=debug)
