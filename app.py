import sqs_listener
import boto3
import json
import logging
import requests
from functions import trigger_scrape_match,trigger_match_lvl_summary, trigger_leg_lvl_summary, trigger_dpl_price, trigger_match_length, telegram_bot_sendtext

access_key_id = 'AKIA467ST2DY3IRHUUWY'
secret_access_key = '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'

# Configure logging
logging.basicConfig(filename='betfair_stream.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Create SQS client
sqs = boto3.client('sqs', region_name='eu-west-1',
                   aws_access_key_id=access_key_id,
                   aws_secret_access_key=secret_access_key)

scrape_failed_dict = {}

class MyListener(sqs_listener.SqsListener):
    def handle_message(self, body, attributes, message_attributes):
        topic = body['Topic']
        json_str = body['Body']
        if topic == 'New Event':
            fixture_id = json_str.get('fixture_id')
            openDate = json_str.get('openDate')
            print(f'{fixture_id}: New Event')
            a_name = json_str.get('a_name')
            b_name = json_str.get('b_name')
            competition = json_str.get('competition')
            eventId = json_str.get('eventId')
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
            logging.info(f"Sent match price request")

        if topic == 'New Market':
            print('New Market')

        if topic == 'Market Finished':
            print('Market Finished')

        if topic == 'Event Finished':
            fixture_id = json_str.get('fixture_id')
            print(f'{fixture_id}: Event Finished')
            a_name = json_str.get('a_name')
            b_name = json_str.get('b_name')
            trigger_scrape_match(a_name, b_name, fixture_id)

        if topic == 'Scrape Failed':
            fixture_id = json_str.get('fixture_id')
            if fixture_id in scrape_failed_dict:
                scrape_failed_dict[fixture_id] += 1
            else:
                scrape_failed_dict[fixture_id] = 1
            selection_a = json_str.get('selection_a')
            print(f'{fixture_id}: Scrape Failed')
            trigger_scrape_match(selection_a, 'Unknown', fixture_id)

        if topic == 'Match Scraped':
            print(f'Match Scraped')
            trigger_match_lvl_summary(body)
            trigger_leg_lvl_summary(body)
            # match scraped
            # trigger match_lvl_summary

        if topic == 'Match Summary Success':
            fixture_id = json_str.get('fixture_id')
            a_name = json_str.get('a_name')
            b_name = json_str.get('b_name')
            if fixture_id not in scraping_dict:
                scraping_dict[fixture_id] = scraping_dict_child
                scraping_dict[fixture_id]['Match'] = 1
            else:
                scraping_dict[fixture_id]['Match'] = 1
            if scraping_dict[fixture_id]['Legs'] == 1 and scraping_dict[fixture_id]['Match'] == 1:
                telegram_bot_sendtext(f'{fixture_id}: Full Scrape Complete')
                url = 'http://127.0.0.1:5000//trigger_related_dpl_price'
                headers = {'Content-Type': 'application/json'}
                data = {"a_name": a_name,
                        "b_name": b_name}
                response = requests.post(url, headers=headers, data=json.dumps(data))
                print(response.status_code)

        if topic == 'Leg Summary Success':
            fixture_id = json_str.get('fixture_id')
            a_name = json_str.get('a_name')
            b_name = json_str.get('b_name')
            if fixture_id not in scraping_dict:
                scraping_dict[fixture_id] = scraping_dict_child
                scraping_dict[fixture_id]['Legs'] = 1
            else:
                scraping_dict[fixture_id]['Legs'] = 1
            if scraping_dict[fixture_id]['Legs'] == 1 and scraping_dict[fixture_id]['Match'] == 1:
                telegram_bot_sendtext(f'{fixture_id}: Full Scrape Complete')
                url = 'http://127.0.0.1:5000//trigger_related_dpl_price'
                headers = {'Content-Type': 'application/json'}
                data = {"a_name": a_name,
                        "b_name": b_name}
                response = requests.post(url, headers=headers, data=json.dumps(data))
                print(response.status_code)


scraping_dict = {}
scraping_dict_child = {'Match': 0, 'Legs': 0}

listener = MyListener('Pooper',
                      region_name='eu-west-1',
                      aws_access_key='AKIA467ST2DY3IRHUUWY',
                      aws_secret_key='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')

listener.listen(