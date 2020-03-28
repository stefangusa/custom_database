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
log_level = None

NODE_NO = sys.argv[1]
NO_NODES = 3
TOPIC_ARN = "arn:aws:sns:eu-west-3:565215661342:Channel"

FILE_PATH = '/etc/custom_database/Node'

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
        logger.debug("Received %s.", json.dumps(message))

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

            logger.debug("Response message: %s.", response)
            response_message = {
                'response': response,
                'id': message['id'],
                'source': NODE_NO,
            }
            publish_sns(response_message, message['source'])

        elif 'replace' in message:
            logger.info("Replacing node %s. Stopping flask application...", message['replace'])
            stop_flask()

            if message['replace'] == NODE_NO:
                replace()

            elif int(NODE_NO) in [(int(message['replace']) - 1) % NO_NODES, (int(message['replace']) + 1) % NO_NODES]:
                logger.info("The replaced node is a neighbour. Starting sending data...")
                try:
                    with open(compute_filename(message['replace']), 'r') as f:
                        content = json.loads(f.read())
                except FileNotFoundError:
                    content = {}

                logger.debug("Sending the replicated data %s.", json.dumps(content))
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

                logger.debug("Sending the resident data to be replicated on the replaced node: %s.", json.dumps(content))
                response = {
                    'no_node': NODE_NO,
                    'content': content
                }
                publish_sns(response, message['replace'], routing_attr='destination')

            logger.info("Finished the process of the replace.")

            logger.debug("Publish a message to signal the replace has finished in order to start the flask application.")
            destination_list = '[' + ', '.join(['\"' + str(node) + '\"' for node in range(NO_NODES)]) + ']'
            publish_sns({'start_flask': NODE_NO}, destination_list, data_type='String.Array')

            logger.debug("Waiting for all the other nodes to finish the replace in order to start the flask application")
            wait_start_flask()

        elif 'scale' in message:
            logger.info("Scaling the cluster %s. Stopping flask application...", message['replace'])
            stop_flask()

            logger.debug("The new size of the cluster: %s", message['scale'])
            NO_NODES = int(message['scale'])

            scale()

            logger.debug("Publish a message to signal the scaling has finished in order to start the flask application.")
            destination_list = '[' + ', '.join(['\"' + str(node) + '\"' for node in range(NO_NODES)]) + ']'
            publish_sns({'start_flask': NODE_NO}, destination_list, data_type='String.Array')

            logger.debug("Waiting for all the other nodes to finish the scaling in order to start the flask application")
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
    logger.debug("Select operation inquired: %s", ','.join([':'.join(key_value) for key_value in attributes]))
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
        logger.warning("Database file not found.")
        return ''


def insert_operation(attributes, base_destination):
    logger.debug("Insert operation inquired: %s", ','.join([':'.join(key_value) for key_value in attributes]))
    (pk, pk_value) = attributes[0]
    try:
        with open(compute_filename(base_destination), 'r') as f:
            content = json.loads(f.read())
            if pk in content and pk_value in content[pk]:
                logger.debug("Primary key %s:%s already exists.", pk, pk_value)
                return 'Error'
    except FileNotFoundError:
        content = {}
        logger.warning("Database file not found.")
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
    logger.debug("Update operation inquired: %s", ','.join([':'.join(key_value) for key_value in attributes]))
    (pk, pk_value) = attributes[0]
    try:
        with open(compute_filename(base_destination), 'r') as f:
            content = json.loads(f.read())
            if pk not in content or pk_value not in content[pk]:
                logger.debug("Primary key %s:%s already exists.", pk, pk_value)
                return 'Error'
    except FileNotFoundError:
        logger.warning("Database file not found.")
        return 'Error'

    if len(attributes) > 1:
        for (key, value) in attributes[1:]:
            content[pk][pk_value].update({key: value})

    with open(compute_filename(base_destination), 'w') as f:
        f.write(json.dumps(content))

    return 'Success'


def delete_operation(attributes, base_destination):
    logger.debug("Delete operation inquired: %s", ','.join([':'.join(key_value) for key_value in attributes]))
    (pk, pk_value) = attributes[0]
    try:
        with open(compute_filename(base_destination), 'r') as f:
            content = json.loads(f.read())
            if pk not in content or pk_value not in content[pk]:
                logger.debug("Primary key %s:%s already exists.", pk, pk_value)
                return 'Error'
    except FileNotFoundError:
        logger.warning("Database file not found.")
        return 'Error'

    del content[pk][pk_value]
    if not content[pk]:
        del content[pk]

    with open(compute_filename(base_destination), 'w') as f:
        f.write(json.dumps(content))

    return 'Success'


def replace():
    logger.info("Replacing this node...")
    retries = 0
    received_files_from = set()

    logger.debug("Receiving data from the neighbours.")
    while retries < 3:
        logger.debug("Polling messages... Retry %d", retries + 1)
        raw_messages = sqs.receive_messages()
        if not raw_messages:
            time.sleep(5)
            retries += 1
            continue

        message = json.loads(json.loads(raw_messages[0].body)['Message'])
        if 'no_node' not in message:
            raw_messages[0].delete()
            continue

        logger.debug("Received %s from %s.", json.dumps(message['content']), message['no_node'])
        received_files_from.add(message['no_node'])
        with open(compute_filename(message['no_node']), 'w') as f:
            f.write(json.dumps(message['content']))
        raw_messages[0].delete()

        if received_files_from == [str((int(NODE_NO) - 1) % NO_NODES), NODE_NO, str((int(NODE_NO) - 1) % NO_NODES)]:
            logger.debug("Received data from both neighbours.")
            sqs.purge()
            break


def scale():
    logger.info("Shutting down the flask application.")
    stop_flask()

    logger.debug("Removing the replicated data on the node.")
    for f in os.listdir('.'):
        if '_repl.cdb' in f:
            logger.debug("Removing %s.", f)
            os.remove(os.path.join('.', f))

    new_content = defaultdict(dict)

    logger.info("Sending data to the new host.")
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

                    logger.debug("Sending %s to node %d.", ','.join([':'.join(key_value) for key_value in attributes]), base_destination)
                    message = {
                        'operation': 'scale',
                        'attributes': attributes,
                        'base_destination': str(base_destination)
                    }

                    publish_sns(message, destination_array, routing_attr='destination', data_type='String.Array')
    except FileNotFoundError:
        pass

    logger.debug("Writing the data which still resides on this node back.")
    with open(compute_filename(NODE_NO), 'w') as f:
        f.write(json.dumps(new_content))

    logger.info("Receiving data from the other nodes in the cluster.")
    retries = 0
    while retries < 3:
        logger.debug("Polling messages... Retry %d", retries + 1)
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

            logger.debug("Received %s.", ','.join([':'.join(key_value) for key_value in attributes]))
            insert_operation(message['attributes'], message['base_destination'])
            raw_message.delete()


def wait_start_flask():
    retries = 0
    responses = 0

    logger.info("Waiting for all the nodes to finish the operation in order to turn on the flask application.")

    while retries < 5:
        logger.debug("Polling messages... Retry %d", retries + 1)
        raw_messages = sqs_resp.receive_messages(MaxNumberOfMessages=10)
        if len(raw_messages) == 0:
            time.sleep(5)
            retries += 1
            continue

        for raw_message in raw_messages:
            message = json.loads(json.loads(raw_message.body)['Message'])
            if 'start_flask' in message:
                responses += 1
                logger.debug("Node %s finished.", message['start_flask'])

            raw_message.delete()

        if responses == NO_NODES:
            logger.info("All the nodes finished the operation. Starting the flask application...")
            break
    else:
        logger.error("At least one node did not finish the operation successfully. Shutting down...")
        sys.exit(1)

    start_flask()


def stop_flask():
    global flask_proc
    try:
        logger.debug("Trying to stop flask application process.")
        flask_proc.terminate()
        logger.debug("Stopped the flask application process.")
    except AttributeError:
        logger.debug("The flask application process is not running.")
        pass


def start_flask():
    global flask_proc

    if flask_proc:
        return

    logger.debug("Creating the flask application process.")
    flask_proc = multiprocessing.Process(target=run_flask, args=(NODE_NO, TOPIC_ARN, log_level), daemon=True)
    logger.debug("Trying to start flask application process.")
    flask_proc.start()
    logger.debug("Started the flask application process.")


def set_logging():
    global logger, handler
    logger = logging.getLogger('custom_database')
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

    logger.info("Starting the custom database...")
    run()


def stop_service():
    logger.debug("Stopping the flask application...")
    stop_flask()
    logger.debug("Flask application stopped.")

    logger.debug("Stopping the custom database...")
    global STOP
    STOP = True
    logger.info("Custom database shut down.")


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print('Usage: python3 database.py <NO. NODES> start | stop | restart | debug')
        sys.exit(0)

    log_level = logging.DEBUG if sys.argv[2] == 'debug' else logging.INFO

    if sys.argv[2] in ['start', 'debug']:
        set_logging()
        start_service()
    elif sys.argv[2] == 'stop':
        set_logging()
        stop_service()
    elif sys.argv[2] == 'restart':
        set_logging()
        stop_service()
        start_service()
    else:
        print('Usage: python3 database.py <NO. NODES> start | stop | restart | debug')


