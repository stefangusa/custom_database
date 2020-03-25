from collections import defaultdict
import json
import logging
import multiprocessing
import os
import sys
import time

from boto3.session import Session
import hashlib

from flask_app import run_flask


LOG_FILE = '/var/log/database.log'

flask_proc = None
logger = None
handler = None

NODE_NO = sys.argv[1]
NO_NODES = 3
TOPIC_ARN = "arn:aws:sns:eu-west-3:565215661342:Channel"

FILE_PATH = './Node'

session = Session(aws_access_key_id='',
                  aws_secret_access_key='')
sqs = session.resource('sqs', region_name='eu-west-3')\
             .Queue("https://sqs.eu-west-3.amazonaws.com/565215661342/Node{}_Messages".format(NODE_NO))
sqs_resp = session.resource('sqs', region_name='eu-west-3') \
    .Queue("https://sqs.eu-west-3.amazonaws.com/565215661342/Node{}_Responses".format(NODE_NO))
sns = session.resource('sns', region_name= 'eu-west-3')\
             .Topic(TOPIC_ARN)

STOP = True


def run():
    global NO_NODES

    retries = 0

    while retries < 3:
        raw_messages = sqs.receive_messages()
        if not raw_messages:
            if STOP:
                retries += 1
            time.sleep(5)
            continue

        retries = 0
        message = json.loads(json.loads(raw_messages[0].body)['Message'])
        if 'operation' in message:
            if message['operation'].lower() == 'select':
                response = select_operation(message['attributes'], message['base_destination'])
            elif message['operation'].lower() == 'insert':
                response = insert_operation(message['attributes'], message['base_destination'])
            elif message['operation'].lower() == 'update':
                response = update_operation(message['attributes'], message['base_destination'])
            elif message['operation'].lower() == 'delete':
                response = delete_operation(message['attributes'], message['base_destination'])
            else:
                continue

            response_message = {
                'response': response,
                'id': message['id'],
                'source': NODE_NO,
            }
            publish_sns(response_message, message['source'])

        elif 'replace' in message:
            stop_flask()

            if message['replace'] == NODE_NO:
                replace()

            elif int(NODE_NO) in [(int(message['replace']) - 1) % NO_NODES, (int(message['replace']) + 1) % NO_NODES]:
                try:
                    with open(compute_filename(message['replace']), 'r') as f:
                        content = json.loads(f.read())
                except FileNotFoundError:
                    content = {}

                response = {
                    'no_node': message['replace'],
                    'content': content
                }
                publish_sns(response, message['replace'], routing_attr='destination')

                try:
                    with open(compute_filename(NODE_NO), 'r') as f:
                        content = json.loads(f.read())
                except FileNotFoundError:
                    content = {}

                response = {
                    'no_node': NODE_NO,
                    'content': content
                }
                publish_sns(response, message['replace'], routing_attr='destination')

            destination_list = '[' + ', '.join(['\"' + str(node) + '\"' for node in range(NO_NODES)]) + ']'
            publish_sns({'start_flask': ''}, destination_list, data_type='String.Array')

            wait_start_flask()

        elif 'scale' in message:
            stop_flask()

            NO_NODES = int(message['scale'])

            scale()

            destination_list = '[' + ', '.join(['\"' + str(node) + '\"' for node in range(NO_NODES)]) + ']'
            publish_sns({'start_flask': ''}, destination_list, data_type='String.Array')

            wait_start_flask()

        raw_messages[0].delete()


def compute_filename(base_destination):
    if base_destination == NODE_NO:
        return '{0}{1}.cdb'.format(FILE_PATH, base_destination)
    else:
        return '{0}{1}_repl.cdb'.format(FILE_PATH, base_destination)


def publish_sns(content, str_value, routing_attr='source', data_type='String'):
    sns.publish(
        Message=json.dumps({'default': json.dumps(content)}),
        MessageStructure='json',
        MessageAttributes={
            routing_attr: {
                'DataType': data_type,
                'StringValue': str_value
            }
        }
    )


def select_operation(attributes, base_destination):
    (pk, pk_value) = attributes[0]
    try:
        with open(compute_filename(base_destination), 'r') as f:
            content = json.loads(f.read())
            if pk not in content or pk_value not in content[pk]:
                return ''
            if len(attributes) > 1:
                for (key, value) in attributes[1:]:
                    if key not in content[pk][pk_value] or content[pk][pk_value] != value:
                        return ''
            return ','.join([':'.join(key_value) for key_value in [(pk, pk_value)] + list(content[pk][pk_value].items())])
    except FileNotFoundError:
        return ''


def insert_operation(attributes, base_destination):
    (pk, pk_value) = attributes[0]
    try:
        with open(compute_filename(base_destination), 'r') as f:
            content = json.loads(f.read())
            if pk in content and pk_value in content[pk]:
                return 'Error'
    except FileNotFoundError:
        content = {}
        pass

    if pk in content:
        content[pk].update({pk_value: {}})
    else:
        content.update({pk: {pk_value: {}}})

    if len(attributes) > 1:
        for (key, value) in attributes[1:]:
            content[pk][pk_value].update({key: value})

    with open(compute_filename(base_destination), 'w') as f:
        f.write(json.dumps(content))

    return 'Success'


def update_operation(attributes, base_destination):
    (pk, pk_value) = attributes[0]
    try:
        with open(compute_filename(base_destination), 'r') as f:
            content = json.loads(f.read())
            if pk not in content or pk_value not in content[pk]:
                return 'Error'
    except FileNotFoundError:
        return 'Error'

    if len(attributes) > 1:
        for (key, value) in attributes[1:]:
            content[pk][pk_value].update({key: value})

    with open(compute_filename(base_destination), 'w') as f:
        f.write(json.dumps(content))

    return 'Success'


def delete_operation(attributes, base_destination):
    (pk, pk_value) = attributes[0]
    try:
        with open(compute_filename(base_destination), 'r') as f:
            content = json.loads(f.read())
            if pk not in content or pk_value not in content[pk]:
                return 'Error'
    except FileNotFoundError:
        return 'Error'

    del content[pk][pk_value]
    if not content[pk]:
        del content[pk]

    with open(compute_filename(base_destination), 'w') as f:
        f.write(json.dumps(content))

    return 'Success'


def replace():
    retries = 0
    received_files_from = set()

    while retries < 3:
        raw_messages = sqs.receive_messages()

        if not raw_messages:
            time.sleep(5)
            retries += 1
            continue

        message = json.loads(json.loads(raw_messages[0].body)['Message'])
        if 'no_node' not in message:
            raw_messages[0].delete()
            continue

        received_files_from.add(message['no_node'])
        with open(compute_filename(message['no_node']), 'w') as f:
            f.write(json.dumps(message['content']))
        raw_messages[0].delete()

        if received_files_from == [str((int(NODE_NO) - 1) % NO_NODES), NODE_NO, str((int(NODE_NO) - 1) % NO_NODES)]:
            sqs.purge()
            break


def scale():
    stop_flask()
    for f in os.listdir('.'):
        if '_repl.cdb' in f:
            os.remove(os.path.join('.', f))

    new_content = defaultdict(dict)

    try:
        with open(compute_filename(NODE_NO), 'r') as f:
            content = json.loads(f.read())
            for pk in content:
                for pk_value in content[pk]:
                    base_destination = int(hashlib.sha256((pk + pk_value).encode('utf-8')).hexdigest(), 16) % NO_NODES
                    attributes = [(pk, pk_value)] + list(content[pk][pk_value].items())

                    if base_destination != int(NODE_NO):
                        destination_array = '["{0}", "{1}", "{2}"]'.format(
                            str((base_destination - 1) % NO_NODES), str(base_destination),
                            str((base_destination + 1) % NO_NODES)
                        )
                    else:
                        destination_array = '["{0}", "{1}"]'.format(
                            str((base_destination - 1) % NO_NODES), str((base_destination + 1) % NO_NODES)
                        )
                        new_content[pk][pk_value] = content[pk][pk_value]

                    message = {
                        'operation': 'scale',
                        'attributes': attributes,
                        'base_destination': str(base_destination)
                    }

                    publish_sns(message, destination_array, routing_attr='destination', data_type='String.Array')
    except FileNotFoundError:
        pass

    with open(compute_filename(NODE_NO), 'w') as f:
        f.write(json.dumps(new_content))

    retries = 0
    while retries < 3:
        raw_messages = sqs.receive_messages(MaxNumberOfMessages=10)
        if len(raw_messages) == 0:
            time.sleep(5)
            retries += 1
            continue

        for raw_message in raw_messages:
            message = json.loads(json.loads(raw_message.body)['Message'])
            if 'operation' not in message or message['operation'] != 'scale':
                raw_message.delete()
                continue

            insert_operation(message['attributes'], message['base_destination'])
            raw_message.delete()


def wait_start_flask():
    retries = 0
    responses = 0

    while retries < 5:
        raw_messages = sqs_resp.receive_messages(MaxNumberOfMessages=10)
        if len(raw_messages) == 0:
            time.sleep(5)
            retries += 1
            continue

        for raw_message in raw_messages:
            message = json.loads(json.loads(raw_message.body)['Message'])

            if 'start_flask' in message:
                responses += 1
            raw_message.delete()

        if responses == NO_NODES:
            break
    else:
        print("Could not restart flask")
        sys.exit(1)

    start_flask()


def stop_flask():
    global flask_proc
    try:
        flask_proc.terminate()
    except AttributeError:
        pass


def start_flask():
    global flask_proc

    if flask_proc:
        return

    flask_proc = multiprocessing.Process(target=run_flask, args=(NODE_NO, TOPIC_ARN), daemon=True)
    flask_proc.start()


def set_logging(log_level):
    global logger, handler
    logger = logging.getLogger()
    logger.setLevel(log_level)

    handler = logging.FileHandler(LOG_FILE)
    handler.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)


def start_service():
    start_flask()

    global STOP
    STOP = False

    run()


def stop_service():
    stop_flask()

    global STOP
    STOP = True


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print('Usage: python3 database.py <NO. NODES> start | stop | restart | debug')
        sys.exit(0)

    if sys.argv[2] == 'start':
        set_logging(logging.INFO)
        start_service()
    elif sys.argv[2] == 'stop':
        set_logging(logging.INFO)
        stop_service()
    elif sys.argv[2] == 'restart':
        set_logging(logging.INFO)
        stop_service()
        start_service()
    elif sys.argv[2] == 'debug':
        set_logging(logging.DEBUG)
        start_service()
    else:
        print('Usage: python3 database.py <NO. NODES> start | stop | restart | debug')


