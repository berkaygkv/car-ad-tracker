# coding=Windows-1256
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException
from selenium.common.exceptions import TimeoutException
import jmespath
import json
import time
from db_setup_remote import Db_Manager
import random
import logging
import pandas as pd


class Crawler():

    def __init__(self, bot):
        # Create or get the logger
        self.logger = logging.getLogger(__name__)

        # set log level
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler('main.log')
        formatter = logging.Formatter(
            '%(asctime)s : %(levelname)s : %(name)s : %(message)s')
        file_handler.setFormatter(formatter)

        # add file handler to logger
        self.logger.addHandler(file_handler)
        self.mng = Db_Manager('crawler')
        self.mng.create_table()
        self.column_names = self.mng.get_column_names()
        self.column_names.remove('adid')
        self.urls_to_be_scraped = self.mng.target_url_number('target_urls')
        self.logger.info(f"STARTED")
        self.js = self.mng.get_config_json()
        self.bot = bot

    def pick_ad(self):
        ad = self.mng.pick_an_ad('target_urls')
        return ad

    def end_session(self):
        self.mng.close_session()
        self.logger.handlers.clear()
        print('Session ended')

    def navigate(self, row):
        ad_id, url, _ = row
        bot = self.bot
        values_dictionary = dict.fromkeys(self.js['table_columns']['car_table'].keys())
        url_split = url.split('/')
        url_split[3], url_split[-1] = 'ilan', 'detay'
        url = '/'.join(url_split)
        try:
            bot.get(url)
        except TimeoutException:
            self.logger.exception(f"Error occurred trying to acces: {url} with EXC: {TimeoutException}")
            time.sleep(1)
            return

        page_wait = '//div[@id="gaPageViewTrackingJson"]'

        try:
            WebDriverWait(bot, 15).until(
                EC.presence_of_all_elements_located((By.XPATH, page_wait)))

        except Exception as ee:
            self.logger.exception("Driver wait error")
            quit()

        js_text = bot.find_element_by_xpath(
            '//div[@id="gaPageViewTrackingJson"]').get_attribute('data-json')
        js = json.loads(js_text)
        dc_1 = jmespath.search('dmpData[*].name', js)
        dc_2 = jmespath.search('dmpData[*].value', js)
        main_dict = dict(zip(dc_1, dc_2))

        if len(dc_2) < 15:
            time.sleep(1)
            if bot.find_elements_by_xpath('//div[@class="error-page-container"]'):
                self.mng.update_row_checked(ad_id)
                self.logger.warning(f"{ad_id} --- {url} ad not active...")
                return

            elif bot.find_elements_by_xpath('//div[@class="classifiedStatusWarning classifiedExpired"]'):
                self.mng.update_row_checked(ad_id)
                self.logger.warning(f"{ad_id} --- {url} ad in review")
                return

            else:
                print("Cannot reading the json")
                self.logger.error(
                    f"{ad_id} --- {url} Cannot reading the json. Will be exited...")
                self.bot.quit()
                self.end_session()
                quit()


        car_parts = {}
        all_original = bot.find_elements_by_xpath('//div[@class="classified-pair custom-area all-original"]')
        if all_original:
            car_parts = dict((k, 'original') for k in self.js['car_parts_columns'])

        else:
            dmg_area = bot.find_elements_by_xpath('//div[@class="car-parts"]//div')
            if dmg_area:
                car_parts = dict((k.get_attribute('class').split(' ')[0], k.get_attribute('class').split(' ')[1]) for k in dmg_area)


        xpath = '//div[@id="classifiedProperties"]//h3[not(contains(text()," Par�a"))]//following-sibling::ul//li'
        selections = bot.find_elements('xpath', xpath)
        binary_conversion = {'selected': 1, '':0}
        properties_dict = dict((selection.text, binary_conversion[selection.get_attribute('class')]) for selection in selections)
        desc = bot.find_element_by_xpath('//div[@id="classifiedDescription"]').text
        title = bot.find_element_by_xpath('//div[@class="classifiedDetailTitle"]//h1').text
        date = bot.find_element_by_xpath('//strong[(contains(text()," Tarihi")) or (contains(text(),"Ad Date"))]//following-sibling::span').text
        values_dictionary.update(
            {
            'ad_id': ad_id,
            'url': url, 
            'title':title, 
            'description':desc, 
            'date_string': date
            }
            )
        values_dictionary.update(car_parts)
        values_dictionary.update(properties_dict)
        values_dictionary.update(main_dict)

        self.mng.write("car_table", values_dictionary, many=False)
        self.mng.update_row_checked(ad_id)
        self.logger.info(
            f"{self.urls_to_be_scraped - self.iteration_count} Left -- <Ad ID>: {ad_id} --- <URL>: {url} scraped...")

    def get_predictions(self, deviation):
        df = self.mng.predict_rows()
        if len(df) > 0:
            #df = df.query(f"deviation < {-1 * (deviation)}")
            df = df.query(f"deviation < 2 & deviation > -2")
            #df = df.query("kimden == 'sahibinden' & fiyat < 305000 & fiyat > 79000 & km < 200000")

            df['fiyat'] = df['fiyat'].map('{:,.0f} TL'.format)
            df['predictions'] = df['predictions'].map('{:,.0f} TL'.format)
            self.logger.info(f"Predictions initiated, {df.shape[0]} significant results...")
            self.logger.info(df[['marka', 'seri', 'model', 'paket', 'fiyat', 'predictions', 'url', 'yil']])
            return df[['marka', 'seri', 'model', 'paket', 'fiyat', 'predictions', 'url', 'title', 'description', 'yil']].values.tolist()

        else:
            self.logger.info(f"No significant listing")
            print('No significant listing')
            return []

    def fetching_loop(self):
        self.iteration_count = 0
        while True:
            try:
                start_time = time.time()
                row = self.pick_ad()
                if row:
                    self.navigate(row)
                    self.iteration_count += 1
                    print(
                        f"\r{self.urls_to_be_scraped - self.iteration_count}", end="")

                else:
                    break
                
                time_passed = time.time() - start_time
                # time_left = random.randint(1500, 2000) / 100 - time_passed
                time_left = random.randint(1500,2000) / 100
                if time_left < 0.0001:
                    time_left = 0
                    
                time.sleep(time_left)
            except Exception:
                self.logger.exception(f"Outer loop exception Occured!: {row[0]}: {row[-2]}")
                time.sleep(1)
                continue

# if __name__ == '__main__':
#     driver = Crawler()
#     driver.fetching_loop()
