import sqs_listener
import boto3
import json
import logging
import requests
from functions import trigger_scrape_match, trigger_match_lvl_summary, trigger_leg_lvl_summary, trigger_dpl_price, \
    telegram_bot_sendtext

access_key_id = 'AKIA467ST2DY3IRHUUWY'
secret_access_key = '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'

# Configure logging
logging.basicConfig(filename='betfair_stream.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

logging.info("Starting App")

# Create SQS client
sqs = boto3.client('sqs', region_name='eu-west-1',
                   aws_access_key_id=access_key_id,
                   aws_secret_access_key=secret_access_key)

scrape_failed_dict = {}


class MyListener(sqs_listener.SqsListener):
    def handle_message(self, body, attributes, message_attributes):
        logging.info(f"Message Received - {body['Body']}")
        topic = body['Topic']
        json_str = body['Body']
        if topic == 'New Event':
            try:
                fixture_id = json_str.get('fixture_id')
                openDate = json_str.get('openDate')
                a_name = json_str.get('a_name')
                b_name = json_str.get('b_name')
                competition = json_str.get('competition')
                eventId = json_str.get('eventId')
                logging.info(f"Received New Event {fixture_id}")
                a_id = json_str.get('a_id')
                b_id = json_str.get('b_id')
                trigger_dpl_price(fixture_id=fixture_id,
                                  openDate=openDate,
                                  eventId=eventId,
                                  competition=competition,
                                  a_name=a_name,
                                  b_name=b_name,
                                  a_id=a_id,
                                  b_id=b_id
                                  )
                data = {"fixture_id": fixture_id,
                        "openDate": openDate,
                        "eventId": eventId,
                        "competition": competition,
                        "a_name": a_name,
                        "b_name": b_name,
                        "a_id": a_id,
                        "b_id": b_id
                        }
                logging.info(f"Sent DPL price request - {json.dumps(data)}")
            except:
                logging.error(f"Error sending New Event price request")

        if topic == 'Event Finished':
            try:
                fixture_id = json_str.get('fixture_id')
                logging.info(f"Received New Event {fixture_id}")
                a_name = json_str.get('a_name')
                b_name = json_str.get('b_name')
                trigger_scrape_match(a_name, b_name, fixture_id)
            except:
                logging.error(f"Error sending Event Finished scrape")

        if topic == 'Scrape Failed':
            try:
                fixture_id = json_str.get('fixture_id')
                if fixture_id in scrape_failed_dict:
                    scrape_failed_dict[fixture_id] += 1
                else:
                    scrape_failed_dict[fixture_id] = 1
                selection_a = json_str.get('selection_a')
                logging.info(f"Received Scrape Failed {fixture_id}")
                trigger_scrape_match(selection_a, 'Unknown', fixture_id)
            except:
                logging.error(f"Error sending Scrape Failed rescrape")

        if topic == 'Match Scraped':
            try:
                logging.info(f"Received Match Scraped")
                trigger_match_lvl_summary(body)
                trigger_leg_lvl_summary(body)
                # match scraped
                # trigger match_lvl_summary
            except:
                logging.error(f"Error sending Match Scraped trigger summary")

        if topic == 'Match Summary Success':
            try:
                fixture_id = json_str.get('fixture_id')
                a_name = json_str.get('a_name')
                b_name = json_str.get('b_name')
                logging.info(f"Received Match Summary {fixture_id}")
                if fixture_id not in scraping_dict:
                    scraping_dict[fixture_id] = scraping_dict_child
                    scraping_dict[fixture_id]['Match'] = 1
                else:
                    scraping_dict[fixture_id]['Match'] = 1
                if scraping_dict[fixture_id]['Legs'] == 1 and scraping_dict[fixture_id]['Match'] == 1:
                    telegram_bot_sendtext(f'{fixture_id}: Full Scrape Complete')
                    url = 'http://ec2-34-240-197-141.eu-west-1.compute.amazonaws.com:5001/trigger_related_dpl_price'
                    headers = {'Content-Type': 'application/json'}
                    data = {"a_name": a_name,
                            "b_name": b_name}
                    response = requests.post(url, headers=headers, data=json.dumps(data))
                    logging.info(f"Sent trigger prices {fixture_id}")
            except:
                logging.error(f"Error in Match Summary")

        if topic == 'Leg Summary Success':
            try:
                fixture_id = json_str.get('fixture_id')
                a_name = json_str.get('a_name')
                b_name = json_str.get('b_name')
                logging.info(f"Received Leg Summary {fixture_id}")
                if fixture_id not in scraping_dict:
                    scraping_dict[fixture_id] = scraping_dict_child
                    scraping_dict[fixture_id]['Legs'] = 1
                else:
                    scraping_dict[fixture_id]['Legs'] = 1
                if scraping_dict[fixture_id]['Legs'] == 1 and scraping_dict[fixture_id]['Match'] == 1:
                    telegram_bot_sendtext(f'{fixture_id}: Full Scrape Complete')
                    url = 'http://ec2-34-240-197-141.eu-west-1.compute.amazonaws.com:5001/trigger_related_dpl_price'
                    headers = {'Content-Type': 'application/json'}
                    data = {"a_name": a_name,
                            "b_name": b_name}
                    response = requests.post(url, headers=headers, data=json.dumps(data))
                    logging.info(f"Sent trigger prices {fixture_id}")
            except:
                logging.error(f"Error in Leg Summary")


scraping_dict = {}
scraping_dict_child = {'Match': 0, 'Legs': 0}

listener = MyListener('Pooper',
                      region_name='eu-west-1',
                      aws_access_key='AKIA467ST2DY3IRHUUWY',
                      aws_secret_key='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')

listener.listen()
