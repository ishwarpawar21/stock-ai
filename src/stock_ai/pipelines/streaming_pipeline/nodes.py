import logging
import json
import time
from upstox_client.rest import ApiException
from websocket import WebSocketApp
# from proto.MarketDataFeed_pb2 import LiveFeed
import time
import upstox_client
import ssl
import boto3
import logging
from pathlib import Path
import logging
from botocore.exceptions import ClientError
from pathlib import Path

def validate_kinesis_stream(aws_access_key_id: str, aws_secret_access_key: str, region_name: str, kinesis_stream_name: str):


    # Set up Boto3 client
    kinesis_client = boto3.client(
        'kinesis',
        aws_access_key_id= aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    stream_name = kinesis_stream_name

    # Check if the Kinesis stream exists
    try:
        response = kinesis_client.describe_stream(StreamName=stream_name)
        logging.info(f"Stream {stream_name} details: {response}")
    except ClientError as e:
        logging.error(f"Error describing stream {stream_name}: {e}")
        raise

    # # Read data from the stream
    # shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    # shard_iterator = kinesis_client.get_shard_iterator(
    #     StreamName=stream_name,
    #     ShardId=shard_id,
    #     ShardIteratorType='TRIM_HORIZON'
    # )['ShardIterator']

    # # Fetch records from the stream
    # records = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)
    # logging.info(f"Records from stream {stream_name}: {records}")
    print(response)
    return response



def load_access_token(token_filepath: str) -> str:
    """Load the access token from a file."""
    with open(token_filepath, 'r') as f:
        access_token_data = json.load(f)
    return access_token_data['access_token']

# def validate_upstox_token(client_id: str, client_secret: str, code: str, redirect_uri: str, grant_type: str) -> str:
#     """Validate the Upstox API v2 token."""
#     api_instance = upstox_client.LoginApi()
#     api_version = '2.0'

#     try:
#         api_response = api_instance.token(
#             api_version,
#             code=code,
#             client_id=client_id,
#             client_secret=client_secret,
#             redirect_uri=redirect_uri,
#             grant_type=grant_type
#         )
#         logging.info("API Response: %s" % api_response)
#         return api_response['access_token']
#     except ApiException as e:
#         logging.error("Exception when calling LoginApi->token: %s\n" % e)
#         raise

def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response


def validate_websocket_data(access_token, instrument_key):
# Create default SSL context
    # ssl_context = ssl.create_default_context()
    # ssl_context.check_hostname = False
    # ssl_context.verify_mode = ssl.CERT_NONE

    # Configure OAuth2 access token for authorization
    configuration = upstox_client.Configuration()

    api_version = '2.0'
    configuration.access_token = access_token
    print("access_token:",access_token)

    # Get market data feed authorization
    response = get_market_data_feed_authorize(
        api_version, configuration)
    
    print(response)
    return response


# def fetch_and_validate_websocket_data(access_token: str, client_id: str, instrument_keys: list) -> list:
#     """Fetch and validate WebSocket data for given instrument keys."""
#     # ws_client = UpstoxWebSocketClient(access_token, client_id, instrument_keys)
    
#     upstox = Upstox(client_id, access_token)

#     validated_data = []

#     def on_message(message):
#         """Handle incoming WebSocket messages."""
#         logging.debug(f"Received message: {message}")
#         if upstox.is_data_streaming():  # Validate data presence
#             validated_data.append(message)  # Collect validated data
#         else:
#             logging.info("No valid data to push.")

#     upstox.on_message = on_message
#     upstox.run()

#     return validated_data

def push_to_kinesis(messages: list, kinesis_stream_name: str, aws_access_key_id: str, aws_secret_access_key: str, region_name: str) -> None:
    """Push data to AWS Kinesis."""
    kinesis_client = boto3.client('kinesis', 
                                  aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key,
                                  region_name=region_name)
    
    for message in messages:
        try:
            kinesis_client.put_record(
                StreamName=kinesis_stream_name,
                Data=json.dumps(message),
                PartitionKey='partitionkey'
            )
            logging.info("Data pushed to Kinesis successfully.")
        except Exception as e:
            logging.error(f"Error pushing data to Kinesis: {e}")

# Timeout settings
NO_DATA_TIMEOUT = 60  # Timeout in seconds

# Timer variable
no_data_timer = None

def reset_no_data_timer(ws):
    global no_data_timer
    if no_data_timer:
        no_data_timer.cancel()
    no_data_timer = Timer(NO_DATA_TIMEOUT, lambda: close_websocket(ws))
    no_data_timer.start()

def close_websocket(ws):
    logging.info("No data received for a while. Closing WebSocket.")
    ws.close()

def on_message(ws, message, kinesis_client, stream_name):
    try:
        data = json.loads(message)
        # Add timestamp or any other necessary fields to data
        data['timestamp'] = datetime.utcnow().isoformat()
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="partition_key"  # Use an appropriate partition key
        )
        logging.info(f"Successfully pushed data to Kinesis: {response}")
        reset_no_data_timer(ws)  # Reset the timer upon receiving a message
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def on_error(ws, error):
    logging.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.info(f"WebSocket closed with status: {close_status_code} and message: {close_msg}")

def on_open(ws):
    logging.info("WebSocket connection opened")
    reset_no_data_timer(ws)  # Start the timer when WebSocket connection opens

def push_to_kinesis_from_websocket():
    # Load AWS credentials
    aws_credentials = load_aws_credentials()

    # Set up Boto3 client
    kinesis_client = boto3.client(
        'kinesis',
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        region_name=aws_credentials['region_name']
    )

    stream_name = aws_credentials['kinesis_stream_name']
    websocket_url = "wss://your-websocket-url"  # Replace with your WebSocket URL

    # Set up WebSocket client
    ws = websocket.WebSocketApp(
        websocket_url,
        on_open=on_open,
        on_message=lambda ws, msg: on_message(ws, msg, kinesis_client, stream_name),
        on_error=on_error,
        on_close=on_close
    )

    # Start WebSocket connection
    ws.run_forever()


# Comment
#https://github.com/upstox/upstox-python/blob/master/examples/websocket/market_data/websocket_client.py