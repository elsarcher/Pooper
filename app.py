import sqs_listener
import boto3
import json
import logging
import requests
from functions import trigger_scrape_match, trigger_match_lvl_summary, trigger_leg_lvl_summary, telegram_bot_sendtext

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

scrape_failed_dict = {}


# Override the default SqsListener to add raw message logging and custom message handling
class CustomSqsListener(sqs_listener.SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        """Process individual message body whether it's from a list or a single object"""
        try:
            topic = body.get('Topic', None)
            body = body.get('Body')
            if not topic:
                logging.error("Message does not contain a topic.")
                return  # Skip processing if there's no topic

            if topic == 'Event Finished':
                try:
                    fixture_id = body.get('fixture_id')
                    logging.info(f"Received Event Finished {fixture_id}")
                    a_name = body.get('a_name')
                    b_name = body.get('b_name')
                    trigger_scrape_match(a_name, b_name, fixture_id)
                except Exception as e:
                    logging.error(f"Error sending Event Finished scrape: {e}")

            elif topic == 'Scrape Failed':
                try:
                    fixture_id = body.get('fixture_id')
                    if fixture_id in scrape_failed_dict:
                        scrape_failed_dict[fixture_id] += 1
                    else:
                        scrape_failed_dict[fixture_id] = 1
                    selection_a = body.get('selection_a')
                    logging.info(f"Received Scrape Failed {fixture_id}")
                    trigger_scrape_match(selection_a, 'Unknown', fixture_id)
                except Exception as e:
                    logging.error(f"Error sending Scrape Failed rescrape: {e}")

            elif topic == 'Match Scraped':
                try:
                    logging.info(f"Received Match Scraped")
                    trigger_match_lvl_summary(body)
                    trigger_leg_lvl_summary(body)
                except Exception as e:
                    logging.error(f"Error sending Match Scraped trigger summary: {e}")

            elif topic == 'Match Summary Success':
                try:
                    fixture_id = body.get('fixture_id')
                    a_name = body.get('a_name')
                    b_name = body.get('b_name')
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

            elif topic == 'Leg Summary Success':
                try:
                    fixture_id = body.get('fixture_id')
                    a_name = body.get('a_name')
                    b_name = body.get('b_name')
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

        except Exception as e:
            logging.error(f"Unexpected error processing message: {e}")


# Initialize tracking dicts
scraping_dict = {}
scraping_dict_child = {'Match': 0, 'Legs': 0}

# Create and start the SQS listener
listener = CustomSqsListener('Pooper',
                             region_name='eu-west-1',
                             aws_access_key_id=access_key_id,
                             aws_secret_key=secret_access_key)

listener.listen()
