import requests
import boto3
from botocore.exceptions import ClientError
import json
import logging

logging.basicConfig(filename='betfair_stream.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def telegram_bot_sendtext(text):
    bot_token = "5862237285:AAEgN1I94fgt8oT39LFNT__-sRfBR9liX0I"
    bot_chatid = '5897205121'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' \
                + bot_chatid + '&parse_mode=Markdown&text=' + text

    response = requests.get(send_text).json()

    return response


def trigger_fixture_query():
    import boto3
    import json
    from botocore.config import Config

    config = Config(retries={'max_attempts': 0})
    lambda_client = boto3.client('lambda',
                                 region_name='eu-west-1',
                                 aws_access_key_id='AKIA467ST2DY3IRHUUWY',
                                 aws_secret_access_key='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',
                                 config=config
                                 )
    lambda_payload = {"Event": "Start"}
    lambda_payload = json.dumps(lambda_payload)
    lambda_client.invoke(FunctionName='fixture_query',
                         InvocationType='Event',
                         Payload=lambda_payload)


def trigger_sim_full_match(fixtureid):
    import boto3
    import json
    from botocore.config import Config

    config = Config(retries={'max_attempts': 0})
    lambda_client = boto3.client('lambda',
                                 region_name='eu-west-1',
                                 aws_access_key_id='AKIA467ST2DY3IRHUUWY',
                                 aws_secret_access_key='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',
                                 config=config,
                                 )
    lambda_payload = {"fixtureid": f"{fixtureid}"}
    lambda_payload = json.dumps(lambda_payload)
    lambda_client.invoke(FunctionName='sim_full_match',
                         InvocationType='Event',
                         Payload=lambda_payload,
                         )


def trigger_dpl_price(fixture_id, openDate, eventId, competition, a_name, b_name, a_id, b_id):
    import boto3
    import json
    from botocore.config import Config

    config = Config(retries={'max_attempts': 0})
    lambda_client = boto3.client('lambda',
                                 region_name='eu-west-1',
                                 aws_access_key_id='AKIA467ST2DY3IRHUUWY',
                                 aws_secret_access_key='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',
                                 config=config,
                                 )
    lambda_payload = {"fixtureid": fixture_id,
                      "openDate": openDate,
                      "eventId": eventId,
                      "competition": competition,
                      "selection_a": a_name,
                      "selection_b": b_name,
                      "a_id": a_id,
                      "b_id": b_id
                      }
    lambda_payload = json.dumps(lambda_payload)
    lambda_client.invoke(FunctionName='price_match',
                         InvocationType='Event',
                         Payload=lambda_payload,
                         )


def trigger_scrape_match(selection_a, selection_b, fixture_id):
    try:
        # Initialize the Lambda client
        lambda_client = boto3.client('lambda',
                                     region_name='eu-west-1',
                                     aws_access_key_id='AKIA467ST2DY3IRHUUWY',
                                     aws_secret_access_key='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')

        # Construct the payload
        lambda_payload = {"selection_a": selection_a, "selection_b": selection_b, "fixture_id": fixture_id}
        # Serialize the JSON payload
        lambda_payload_str = json.dumps(lambda_payload)

        # Invoke the Lambda function
        response = lambda_client.invoke(FunctionName='lambda_scrape_match',
                                        InvocationType='Event',
                                        Payload=lambda_payload_str)

    except ClientError as e:
        logging.info(f"Error Invoking Lambda scrape match {fixture_id}")

def trigger_match_lvl_summary(json_payload):
    try:
        # Initialize the Lambda client
        lambda_client = boto3.client('lambda',
                                     region_name='eu-west-1',
                                     aws_access_key_id='AKIA467ST2DY3IRHUUWY',
                                     aws_secret_access_key='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')

        # Serialize the JSON payload
        lambda_payload = json.dumps(json_payload)

        # Invoke the Lambda function
        response = lambda_client.invoke(FunctionName='match_lvl_summary',
                                        InvocationType='Event',
                                        Payload=lambda_payload)

    except ClientError as e:
        print(f"Error invoking Lambda function: {e.response['Error']['Message']}")


def trigger_leg_lvl_summary(json_payload):
    try:
        # Initialize the Lambda client
        lambda_client = boto3.client('lambda',
                                     region_name='eu-west-1',
                                     aws_access_key_id='AKIA467ST2DY3IRHUUWY',
                                     aws_secret_access_key='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')

        # Serialize the JSON payload
        lambda_payload = json.dumps(json_payload)

        # Invoke the Lambda function
        response = lambda_client.invoke(FunctionName='leg_lvl_summary',
                                        InvocationType='Event',
                                        Payload=lambda_payload)

    except ClientError as e:
        print(f"Error invoking Lambda function: {e.response['Error']['Message']}")


