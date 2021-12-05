from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from db_setup_remote import Db_Manager
import logging
import time
from selenium.common.exceptions import TimeoutException


class Spider():

    def __init__(self, bot):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler('main.log')
        formatter = logging.Formatter(
            '%(asctime)s : %(levelname)s : %(name)s : %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.mng = Db_Manager('spider')
        self.mng.create_table()
        self.logger.info(f"STARTED")
        self.bot = bot

    def find_ads(self):
        bot = self.bot
        bot.get(
            'https://www.sahibinden.com/en/kategori-showcase?viewType=Gallery&pagingSize=50&category=3530')
        ads_found = bot.find_element_by_xpath(
            '//div[@class="result-text"]').find_element_by_xpath('.//span').text
        ads_found = int(ads_found.replace(',', ''))-10
        categories = bot.find_elements_by_xpath('//li[@class="cl2"]')
        category_links = [cat.find_element_by_xpath(
            './/a').get_attribute('href') for cat in categories]
        navi_buttons = bot.find_element_by_xpath(
            '//ul[@class="pageNaviButtons"]')
        last_page_element = navi_buttons.find_element_by_xpath(
            './/li//input[@id="currentPageValue"]')
        current_page = last_page_element.get_attribute('value')
        
        for idx, link in enumerate(category_links):
            bot.get(link)
            time.sleep(1)
            navi_buttons = bot.find_element_by_xpath(
                '//ul[@class="pageNaviButtons"]')
            last_page_element = navi_buttons.find_elements_by_xpath('.//li')[-2]
            last_page = last_page_element.find_element_by_xpath(
                '//a').get_attribute('title')

            while True:
                table_xp = '//table[@id="searchResultsGallery"]'
                WebDriverWait(bot, 25).until(
                    EC.presence_of_all_elements_located((By.XPATH, table_xp)))
                table = bot.find_element_by_xpath(table_xp)
                hrefs = table.find_elements_by_xpath(
                    './/td[@class="searchResultsGalleryContent"]')

                listings = []
                for href in hrefs:
                    ad_id = href.find_element_by_xpath(
                        './/following-sibling::div[@class="searchResultsClassifiedId"]').text.replace('#', '')
                    link = href.find_element_by_xpath(
                        './/following-sibling::a').get_attribute('href')
                    listings.append([int(ad_id), link, None])
                self.mng.write('target_urls', listings)
                self.logger.info(
                    f"{current_page} / {last_page} --- # of new ads: {number_of_new_ads} ---- # of Total unchecked URLs: {total_new_ads}")
                next_button_obj = bot.find_elements_by_xpath(
                    '//a[(@class="prevNextBut") and (@title="Sonraki")]')
                if not next_button_obj:
                    break


                next_button = next_button_obj[0]
                loc_nxt = next_button.location_once_scrolled_into_view
                loc_y = loc_nxt['y'] - 70
                bot.execute_script(f"window.scrollBy(0, {loc_y})")
                bot.find_element_by_xpath('//a[@title="Sonraki"]').click()
                loading_element = bot.find_element_by_xpath(
                    '//div[@class="opening"]')

                try:
                    WebDriverWait(bot, 25).until(
                        EC.invisibility_of_element_located(loading_element))
                except TimeoutException:
                    self.logger.exception(f"Loading element Timeout Error")
                    raise
                    self.logger.info(
                        f"Categories Left: {len(category_links) - idx}'")

    def update_ads(self, stop_first_encounter=False):
        url = 'https://www.sahibinden.com/kategori-vitrin?viewType=Gallery&pagingOffset=50&pagingSize=50&category=3530&sorting=date_desc'
        bot = self.bot
        bot.get(url)
        time.sleep(1)
        navi_buttons = bot.find_element_by_xpath(
            '//ul[@class="pageNaviButtons"]')
        last_page_element = navi_buttons.find_elements_by_xpath('.//li')[-2]
        last_page = last_page_element.find_element_by_xpath(
            '//a').get_attribute('title')

        while True:
            navi_buttons = bot.find_element_by_xpath(
                '//ul[@class="pageNaviButtons"]')
            last_page_element = navi_buttons.find_element_by_xpath(
                './/li//input[@id="currentPageValue"]')
            current_page = last_page_element.get_attribute('value')
            table_xp = '//table[@id="searchResultsGallery"]'
            try:
                WebDriverWait(bot, 25).until(
                    EC.presence_of_all_elements_located((By.XPATH, table_xp)))
            except TimeoutException:
                self.logger.exception(
                    f"Presence of all elements Timeout Error")
                raise

            table = bot.find_element_by_xpath(table_xp)
            hrefs = table.find_elements_by_xpath(
                './/td[@class="searchResultsGalleryContent"]')

            listings = []
            ad_id_list = []
            for href in hrefs:
                ad_id = href.find_element_by_xpath(
                    './/following-sibling::div[@class="searchResultsClassifiedId"]').text.replace('#', '')
                link = href.find_element_by_xpath(
                    './/following-sibling::a').get_attribute('href')
                ad_id_list.append(ad_id)
                listings.append([int(ad_id), link, None])

            number_of_new_ads = len(ad_id_list) - \
                len(self.mng.check_id(ad_id_list))
            self.mng.write('target_urls', listings)
            total_new_ads = self.mng.target_url_number('target_urls')
            self.logger.info(
                f"{current_page} / {last_page} --- # of new ads: {number_of_new_ads} ---- # of Total unchecked URLs: {total_new_ads}")

            next_button_obj = bot.find_elements_by_xpath(
                '//a[(@class="prevNextBut") and (@title="Sonraki")]')
            if not next_button_obj:
                break

            if stop_first_encounter:
                checked_id_len = len(self.mng.check_id(ad_id_list))
                if checked_id_len == len(listings):
                    self.logger.info(f"{checked_id_len} of {len(listings)} listings are identical")
                    break

            next_button = next_button_obj[0]
            loc_nxt = next_button.location_once_scrolled_into_view
            loc_y = loc_nxt['y'] - 70
            bot.execute_script(f"window.scrollBy(0, {loc_y})")
            bot.find_element_by_xpath('//a[@title="Sonraki"]').click()
            loading_element = bot.find_element_by_xpath(
                '//div[@class="opening"]')

            try:
                WebDriverWait(bot, 25).until(
                    EC.invisibility_of_element_located(loading_element))
            except TimeoutException:
                self.logger.exception(f"Loading element Timeout Error")
                raise

    def end_session(self):
        self.mng.close_session()
        self.logger.info(f"Session ended")
        logging.shutdown()
        self.logger.handlers.clear()


# if __name__ == '__main__':
#     driver = Spider()
#     driver.update_ads()
#     driver.end_session()
