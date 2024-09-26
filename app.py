import sqs_listener
import boto3
import json
import logging
import requests
from functions import trigger_scrape_match, trigger_match_lvl_summary, trigger_leg_lvl_summary, trigger_dpl_price, \
    telegram_bot_sendtext

# AWS credentials
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

# Dictionary for tracking failed scrapes
scrape_failed_dict = {}

class MyListener(sqs_listener.SqsListener):
    def handle_message(self, body, attributes, message_attributes):
        try:
            logging.info(f"Raw Message Received: {body}")
            
            # Deserialize the body into a Python dictionary
            message_body = json.loads(body['Body'])
            logging.info(f"Parsed Message Body: {message_body}")
            
            # Extract topic from message body
            topic = message_body.get('Topic', None)
            
            if not topic:
                logging.error("Message does not contain a topic.")
                return  # Skip processing if there's no topic

            # Handling 'New Event' topic
            if topic == 'New Event':
                try:
                    fixture_id = message_body.get('fixture_id')
                    openDate = message_body.get('openDate')
                    a_name = message_body.get('a_name')
                    b_name = message_body.get('b_name')
                    competition = message_body.get('competition')
                    eventId = message_body.get('eventId')
                    logging.info(f"Received New Event {fixture_id}")
                    a_id = message_body.get('a_id')
                    b_id = message_body.get('b_id')
                    
                    # Trigger DPL price function
                    trigger_dpl_price(fixture_id=fixture_id,
                                      openDate=openDate,
                                      eventId=eventId,
                                      competition=competition,
                                      a_name=a_name,
                                      b_name=b_name,
                                      a_id=a_id,
                                      b_id=b_id)
                    data = {
                        "fixture_id": fixture_id,
                        "openDate": openDate,
                        "eventId": eventId,
                        "competition": competition,
                        "a_name": a_name,
                        "b_name": b_name,
                        "a_id": a_id,
                        "b_id": b_id
                    }
                    logging.info(f"Sent DPL price request - {json.dumps(data)}")
                except Exception as e:
                    logging.error(f"Error sending New Event price request: {e}")

            # Handling 'Event Finished' topic
            elif topic == 'Event Finished':
                try:
                    fixture_id = message_body.get('fixture_id')
                    logging.info(f"Received Event Finished {fixture_id}")
                    a_name = message_body.get('a_name')
                    b_name = message_body.get('b_name')
                    trigger_scrape_match(a_name, b_name, fixture_id)
                except Exception as e:
                    logging.error(f"Error sending Event Finished scrape: {e}")

            # Handling 'Scrape Failed' topic
            elif topic == 'Scrape Failed':
                try:
                    fixture_id = message_body.get('fixture_id')
                    if fixture_id in scrape_failed_dict:
                        scrape_failed_dict[fixture_id] += 1
                    else:
                        scrape_failed_dict[fixture_id] = 1
                    selection_a = message_body.get('selection_a')
                    logging.info(f"Received Scrape Failed {fixture_id}")
                    trigger_scrape_match(selection_a, 'Unknown', fixture_id)
                except Exception as e:
                    logging.error(f"Error sending Scrape Failed rescrape: {e}")

            # Handling 'Match Scraped' topic
            elif topic == 'Match Scraped':
                try:
                    logging.info(f"Received Match Scraped")
                    trigger_match_lvl_summary(message_body)
                    trigger_leg_lvl_summary(message_body)
                except Exception as e:
                    logging.error(f"Error sending Match Scraped trigger summary: {e}")

            # Handling 'Match Summary Success' topic
            elif topic == 'Match Summary Success':
                try:
                    fixture_id = message_body.get('fixture_id')
                    a_name = message_body.get('a_name')
                    b_name = message_body.get('b_name')
                    logging.info(f"Received Match Summary {fixture_id}")
                    if fixture_id not in scraping_dict:
                        scraping_dict[fixture_id] = scraping_dict_child.copy()
                        scraping_dict[fixture_id]['Match'] = 1
                    else:
                        scraping_dict[fixture_id]['Match'] = 1
                    
                    if scraping_dict[fixture_id]['Legs'] == 1 and scraping_dict[fixture_id]['Match'] == 1:
                        telegram_bot_sendtext(f'{fixture_id}: Full Scrape Complete')
                        url = 'http://ec2-52-50-252-157.eu-west-1.compute.amazonaws.com:5001/trigger_related_dpl_price'
                        headers = {'Content-Type': 'application/json'}
                        data = {"a_name": a_name, "b_name": b_name}
                        response = requests.post(url, headers=headers, data=json.dumps(data))
                        logging.info(f"Sent trigger prices {fixture_id}")
                except Exception as e:
                    logging.error(f"Error in Match Summary: {e}")

            # Handling 'Leg Summary Success' topic
            elif topic == 'Leg Summary Success':
                try:
                    fixture_id = message_body.get('fixture_id')
                    a_name = message_body.get('a_name')
                    b_name = message_body.get('b_name')
                    logging.info(f"Received Leg Summary {fixture_id}")
                    if fixture_id not in scraping_dict:
                        scraping_dict[fixture_id] = scraping_dict_child.copy()
                        scraping_dict[fixture_id]['Legs'] = 1
                    else:
                        scraping_dict[fixture_id]['Legs'] = 1
                    
                    if scraping_dict[fixture_id]['Legs'] == 1 and scraping_dict[fixture_id]['Match'] == 1:
                        telegram_bot_sendtext(f'{fixture_id}: Full Scrape Complete')
                        url = 'http://ec2-52-50-252-157.eu-west-1.compute.amazonaws.com:5001/trigger_related_dpl_price'
                        headers = {'Content-Type': 'application/json'}
                        data = {"a_name": a_name, "b_name": b_name}
                        response = requests.post(url, headers=headers, data=json.dumps(data))
                        logging.info(f"Sent trigger prices {fixture_id}")
                except Exception as e:
                    logging.error(f"Error in Leg Summary: {e}")

        except json.JSONDecodeError as e:
            logging.error(f"Error parsing message body: {e}")
        except Exception as e:
            logging.error(f"Unexpected error handling message: {e}")

# Initialize tracking dicts
scraping_dict = {}
scraping_dict_child = {'Match': 0, 'Legs': 0}

# Create and start the SQS listener
listener = MyListener('Pooper',
                      region_name='eu-west-1',
                      aws_access_key=access_key_id,
                      aws_secret_key=secret_access_key)

listener.listen()
