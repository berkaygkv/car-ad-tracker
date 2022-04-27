from driver_setup import init_driver
from spider_remote import Spider
from crawler_remote import Crawler
import logging
from db_setup_remote import Db_Manager
import time
import slack
import re
import jmespath


def get_labeled_ads():
    user_client = slack.WebClient(token=<TOKEN_HERE>)
    reactions_data = user_client.reactions_list(channel='C01DA5NPHDH').data
    filtered_reactions = jmespath.search('items[*].message.text', reactions_data)
    if filtered_reactions:
        reacted_urls = [re.compile('(?=<).+(?=\|)').search(k).group().replace('<', '') for k in filtered_reactions if re.compile('(?=<).+(?=\|)').search(k)]
        timestamp_list = jmespath.search('items[*].message.ts', reactions_data)
        for i in enumerate(timestamp_list):
            try:
                user_client.chat_delete(channel='C01DA5NPHDH', ts=i)
            except:
                pass
            time.sleep(1.5)

    else:
        reacted_urls = []

    return reacted_urls

def delete_messages():
    user_client = slack.WebClient(token=<TOKEN_HERE>)
    user_client = user_client
    message_data = user_client.conversations_history(channel='C01DA5NPHDH').data
    active_messages = message_data['messages']

    timestamp_list = [m['ts'] for m in active_messages]
    for d, i in enumerate(timestamp_list):
        try:
            user_client.chat_delete(channel='C01DA5NPHDH', ts=i)
        except:
            pass
        time.sleep(1.5)


client  = slack.WebClient(token=<TOKEN_HERE>)
mng = Db_Manager('spider')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('main.log')
formatter = logging.Formatter(
    '%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

minutes = 3
deviation = -22222
user_profile = r"/root"
#user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36"
user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
preset_time = 60 * minutes

logger.info(f"Session started with User profile: {user_profile} -- User agent:{user_agent} -- Preset sleep time: {minutes} minutes")
driver = init_driver(user_agent=user_agent, user_profile=user_profile, load_image=1, headless=True, debug_port=False)


while True:
    spider = Spider(driver)
    while True:
        start_time = time.time()
        spider.update_ads(stop_first_encounter=True)
        # spider.find_ads()
        if mng.target_url_number('target_urls'):
            logger.info(f"Loop broken with <{mng.target_url_number('target_urls')}> NULL URLs")
            spider.end_session()
            #driver.delete_all_cookies()
            #logger.info(f"Cookies Deleted...")
            break

        labeled_ad_urls = get_labeled_ads()
        if labeled_ad_urls:
            fetching_time = time.time()
            logger.info(f"No new ads... <{len(labeled_ad_urls)}> labels being fetched.")
            mng.update_labels(labeled_ad_urls)
            fetching_time = time.time() - fetching_time
            time_left = preset_time - fetching_time
            if time_left < 0.001:
                time_left = 0.01
            logger.info(f"Label fetch is done. Loop will sleep for {time_left}")
            time.sleep(time_left)

        else:
            logger.info(f"No new ads... loop will sleep for {preset_time}")
            time.sleep(preset_time)


    crawler = Crawler(driver)
    crawler.fetching_loop()
    #driver.delete_all_cookies()
    #logger.info(f"Cookies Deleted...")
    # ids = crawler.get_predictions(deviation=deviation)
    # entries = []

    # for marka, seri, model, paket, fiyat, prediction, url, title, description, yil in ids:
    #     pr = [f":pushpin:\n\n*{title}*\n>\n\n*{yil} {marka} - {seri} - {model} - {paket}*\n>\n\n```{description}```\n\nFiyat: {fiyat}\nTahmini Fiyat: `{prediction}`\nURL: <{url}|Arac *Link*:bomb:>\n\n--------------------------------------------------"]
    #     entries.append(pr)

    # if entries:
    #     entries.reverse()
    #     for i in entries:
    #         msg = client.chat_postMessage(channel='#upwork', text=i[0], link_names = 1)
        
    crawler.end_session()
    end_time = time.time()
    time_passed = end_time - start_time
    sleep_time = preset_time - time_passed
    
    if sleep_time > 0:
        logger.info(f"Crawler finished... loop will sleep for {sleep_time} seconds")
        time.sleep(sleep_time)

    else:
        logger.info(f"Crawler finished and exceeds the preset time by {-1*(sleep_time)} seconds. No sleep.")

    time.sleep(1)


