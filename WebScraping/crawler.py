from asyncio.windows_events import NULL
from decimal import ROUND_UP
from pickle import FALSE, TRUE
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import os
import re
import datetime
import time as t

class IranketabCrawler:
    def __init__(self):
        self.baseURL = 'https://www.iranketab.ir'
        self.data_path = './Data'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36', 
            'accept-language': 'en-US'
        }
        self.dataframe_names = ['books_url', 'book', 'publisher', 'outher', 'translator', 'award','book_summary', 'tag']
        self.books_url = pd.DataFrame(columns=['id', 'url'])
        self.book = pd.DataFrame(
            columns=['id', 'url_id', 'title', 'en_title', 'price', 'ghat', 'discount', 'ISBN', 'cover', 'page_count', 'solar_publish_year',
                     'ad_publish_year', 'edition', 'fastest_delivery', 'rate', 'available'])
        self.publisher = pd.DataFrame(columns=['id_book', 'name'])
        self.outher = pd.DataFrame(columns=['id_book', 'name'])
        self.translator = pd.DataFrame(columns=['id_book', 'name'])
        self.award = pd.DataFrame(columns=['id_book', 'award_name'])
        self.book_summary = pd.DataFrame(columns=['book_id', 'text'])
        self.tag = pd.DataFrame(columns=['book_id', 'tag_id', 'tag'])
        self.flag = 0
        self.flag_book = 0
        self.tag_flag = 0
    
    def start_crawling(self):
        self.create_data_directory()
        
        for df_name in self.dataframe_names:
            self.load_csv(df_name)

        # get all books url    
        # self.crawl_pagination()

        self.crawl_books_page()
       
    def create_data_directory(self):
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
            
    def load_csv(self, df_name):
        file_path = os.path.join(self.data_path, df_name + '.csv')

        if not os.path.exists(file_path):
            df_columns = getattr(self, df_name).columns
            df = pd.DataFrame(columns=df_columns)
            df.to_csv(file_path, index=False)

        setattr(self, df_name, pd.read_csv(file_path))
    
    def save_csv(self, df_name):
        file_path = os.path.join(self.data_path, df_name + '.csv')
        df = getattr(self, df_name)
        df.to_csv(file_path, index=False)

    
    def save_all_csv(self, df_name):
        file_path = os.path.join(self.data_path, df_name + '.csv')
        df = getattr(self, df_name)
        df.to_csv(file_path, mode='a', index=False, header=False)    
    
    def scrap_every_page(self, page):
        new_books_url = pd.DataFrame(columns=['book_id', 'book_url'])
        page_url = self.baseURL + '/book?pagenumber='+ str(page)
        pagination_req_failed_count = 0
        while pagination_req_failed_count < 3:
            try:
                response = requests.get(page_url, headers=self.headers)
                if response.status_code != 200:
                    raise Exception(f"status code is: {response.status_code}")
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                books = soup.find_all('div' , attrs={'class' : 'col-lg-6 col-md-6 col-xs-12'})
                
                for book in books:
                    book_url = book.find('h4' , attrs={'class' : 'product-name-title'}).find('a')['href']
                    book_id = int(book_url.split('/')[2].split('-')[0])
                    new_books_url.loc[len(new_books_url)] = [book_id, book_url]
                                
                return new_books_url
                
            except Exception as e:
                pagination_req_failed_count += 1
                print(e)

    def crawl_pagination(self):
        print("get_all_books_url started")
        
        url = self.baseURL + '/book'
        req_failed_count = 0

        while req_failed_count < 3:
            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code != 200:
                    raise Exception(f"status code is: {response.status_code}")

                soup = BeautifulSoup(response.content, 'html.parser')
      
                pagination_count = int(soup.find('ul' , attrs={'class' : 'pagination'}).find_all('li')[10].find('a')['url'].split('=')[1].split('&')[0])
                
                with ThreadPoolExecutor(max_workers=50) as executor:
                    futures = (executor.submit(self.scrap_every_page, page) for page in range(1, pagination_count+1))
                    for future in as_completed(futures):
                        new_books_url = future.result()   

                        if new_books_url is not None:
                            for i in range(len(new_books_url)):
                                self.books_url.loc[len(self.books_url)] = [
                                    new_books_url['book_id'][i], 
                                    new_books_url['book_url'][i]
                                    ]
                            print(len(self.books_url))
                            self.save_csv('books_url')
                        
                break

            except Exception as e:
                req_failed_count += 1
                print(e)

        print("get_all_books_url finished")

    def scrap_every_book_page(self, url):
        page_url = self.baseURL + url + "abc"
        pagination_req_failed_count = 0

        while pagination_req_failed_count < 3:
            try:
                response = requests.get(page_url, headers=self.headers)
                if response.status_code != 200:
                    raise Exception(f"status code is: {response.status_code}")
                
                book_page = BeautifulSoup(response.content, 'html.parser')
                book_id = int(url.split('/')[2].split('-')[0])        

                return book_id, book_page
                
            except Exception as e:
                pagination_req_failed_count += 1
                print("1:",e)

    def crawl_books_page(self):
        print("get_all_books_page started")
        
        req_failed_count = 0
        while req_failed_count < 3:
            try:
                new_url = {}
                print("count of loaded url: ", len(self.books_url))
                for i in range(len(self.books_url)):
                    new_url[self.books_url.iloc[i]['id']] = self.books_url.iloc[i]['url']
                print("count of loaded url: ", len(new_url))

                #for summary that you need to create like it for your data

                # for i in range(len(self.book_summary)):
                #     if self.book_summary.iloc[i]['book_id'] in new_url:
                #         new_url.pop(self.book_summary.iloc[i]['book_id'])
                # print("count of new url: ", len(new_url))
                # self.flag += len(self.book_summary)
                # self.book_summary = pd.DataFrame({'book_id' : [], 'text': []})

                #for get all book
                for i in range(len(self.book)):
                    if self.book.iloc[i]['url_id'] in new_url:
                        new_url.pop(self.book.iloc[i]['url_id'])

                self.flag_book += len(self.book)
                
                self.book = pd.DataFrame({'id': [], 'url_id': [], 'title': [], 'en_title': [], 'price': [], 'ghat': [], 'discount': [], 'ISBN': [], 'cover': [], 'page_count': [], 'solar_publish_year': [],
                     'ad_publish_year': [], 'edition': [], 'fastest_delivery': [], 'rate': [], 'available': []})
                self.publisher = pd.DataFrame({'id_book': [], 'name': []})
                self.outher = pd.DataFrame({'id_book': [], 'name': []})
                self.translator = pd.DataFrame({'id_book': [], 'name': []})
                self.award = pd.DataFrame({'id_book': [], 'award_name': []})

                #get all tag
                # for i in range(len(self.tag)):
                #     if self.tag.iloc[i]['book_id'] in new_url:
                #         new_url.pop(self.tag.iloc[i]['book_id'])
                # print("count of new url: ", len(new_url))
                # self.tag_flag += len(self.tag)
                # self.tag = pd.DataFrame(columns=['book_id', 'tag_id', 'tag'])

                with ThreadPoolExecutor(max_workers=25) as executor:
                    futures = (executor.submit(self.scrap_every_book_page, url) for url in new_url.values())
                                           
                    for future in as_completed(futures):
                        book_id, book_page = future.result()   

                        self.crawl_books_summary(book_id, book_page)
                        self.crawl_books(book_id, book_page)
                        self.crawl_books_tags(book_id, book_page)
                
                break

            except Exception as e:
                req_failed_count += 1
                print("2:",e)

        if len(self.book) > 0:
            self.flag_book += len(self.book)
            print(self.flag_book) 
            self.save_all_csv('book')
            self.save_all_csv('publisher')
            self.save_all_csv('outher')
            self.save_all_csv('translator')
            self.save_all_csv('award')

        if len(self.tag) > 0:
            self.flag += len(self.book_summary)
            print(self.flag)       
            self.save_all_csv('book_summary')

        if len(self.tag) > 0:
            self.tag_flag += len(self.tag)
            print(self.tag_flag)       
            self.save_summary_csv('tag')

        print("get_all_books_pages finished")

    def crawl_books_summary(self, book_id, book_page):
        summary = book_page.find('div' , attrs={'class' : 'product-description'})
        if summary is not None:
            summary_text = summary.text
            if len(self.book_summary) == 100:
                self.flag += 100
                print(self.flag)
                self.save_all_csv('book_summary')
                self.book_summary = pd.DataFrame({'book_id' : [], 'text': []})
            self.book_summary.loc[len(self.book_summary)] = [
                book_id,
                summary_text
            ]

        else:
           if len(self.book_summary) == 100:
                self.flag += 100
                print(self.flag)
                self.save_all_csv('book_summary')
                self.book_summary = pd.DataFrame({'book_id' : [], 'text': []})
           self.book_summary.loc[len(self.book_summary)] = [
                book_id,
                None
            ]
    

    def crawl_books(self, book_id, book_page):
        books_container = book_page.find('div' , attrs={'class' : 'product-container well clearfix'})
        books_container_item = books_container.find_all('div' , attrs={'class' : 'clearfix'})
        books_container_item_nois = books_container.find_all('div' , attrs={'class' : 'row clearfix'})

        for item in books_container_item_nois:
            if item in books_container_item:
                books_container_item.remove(item)

        for i in range(len(books_container_item)):
            url_id = book_id

            if i == 0:
                title = books_container_item[i].find('h1' , attrs={'class' : 'product-name'}).text
            else:
                title = books_container_item[i].find('div' , attrs={'class' : 'product-name'}).text

            en_title_tag = books_container_item[i].find('div' , attrs={'class' : 'product-name-englishname'})
            
            if en_title_tag is not None:  
                en_title =  en_title_tag.text 
            else:
                en_title = None

            product_table_tr = books_container_item[i].find('table' , attrs={'class' : 'product-table'}).find_all('tr')

            id= translator= isbn= ghat= page_count= solar_publish_year= ad_publish_year= cover= edition= fastest_delivery = None
            
            for tr in product_table_tr:
                td = tr.find_all('td')
                if 'کد کتاب' in td[0].text:
                    id = int(td[1].text)
                elif 'مترجم' in td[0].text:
                    translator = td[1].text
                elif 'شابک' in td[0].text:
                    isbn = td[1].text
                elif 'قطع' in td[0].text:
                    ghat = td[1].text
                elif 'تعداد صفحه' in td[0].text:
                    page_count = td[1].text
                elif 'سال انتشار شمسی' in td[0].text:
                    solar_publish_year = td[1].text
                elif 'سال انتشار میلادی' in td[0].text:
                    ad_publish_year = td[1].text
                elif 'نوع جلد' in td[0].text:
                    cover = td[1].text
                elif 'سری چاپ' in td[0].text:
                    edition = td[1].text
                elif 'زودترین زمان ارسال' in td[0].text:
                    fastest_delivery = td[1].text

            price_tag = books_container_item[i].find('span' , attrs={'class' : 'price price-special'})

            if price_tag is not None:  
                price =  price_tag.text  
            else:
                price_tag = books_container_item[i].find('span' , attrs={'class' : 'price'})
                if price_tag is not None: 
                    price =  price_tag.text  
                else:
                    price = None

            discount_tag = books_container_item[i].find('ul' , attrs={'class' : 'clearfix'}).find('li').find('div')
            if discount_tag is not None:  
                discount =  discount_tag.text.split()[0]  
            else:
                discount = None
            
            rate_tag = books_container_item[i].find('div' , attrs={'class' : 'my-rating'})
            if rate_tag is not None:  
                rate =  rate_tag['data-rating'] 
            else:
                rate = None

            available_tag = books_container_item[i].find('li' , attrs={'class' : 'exists-book'})
            if available_tag is not None: 
                if 'موجود' in available_tag.text:
                    available = 1
                else:
                    available = 0
            else:
                available = 0

            publisher_tag = books_container_item[i].find('div' , attrs={'class' : 'col-xs-12 prodoct-attribute-items'}).find('a')
            if publisher_tag is not None: 
                publisher = publisher_tag.text
            else:
                publisher = None    

            auther_main_tag = books_container_item[i].find_all('div' , attrs={'class' : 'col-xs-12 prodoct-attribute-items'})       
            outher = []
            for auther_m in auther_main_tag:
                if 'نویسنده' in auther_m.find('span').text:
                    for writer in auther_m.find_all('a'):
                        outher.append(writer.text)

            award_tag = books_container_item[i].find('div' , attrs={'class' : 'product-features'})
            award = []
            if award_tag is not None: 
                for aw in award_tag.find_all('h4'):
                    award.append(aw.text)
            else:
                award = []
                
            if len(self.book) == 100:
                self.flag_book += 100
                print(self.flag_book)
                self.save_all_csv('book')
                self.save_all_csv('publisher')
                self.save_all_csv('outher')
                self.save_all_csv('translator')
                self.save_all_csv('award')
                self.book = pd.DataFrame({'id': [], 'url_id': [], 'title': [], 'en_title': [], 'price': [], 'ghat': [], 'discount': [], 'ISBN': [], 'cover': [], 'page_count': [], 'solar_publish_year': [],
                     'ad_publish_year': [], 'edition': [], 'fastest_delivery': [], 'rate': [], 'available': []})
                self.publisher = pd.DataFrame({'id_book': [], 'name': []})
                self.outher = pd.DataFrame({'id_book': [], 'name': []})
                self.translator = pd.DataFrame({'id_book': [], 'name': []})
                self.award = pd.DataFrame({'id_book': [], 'award_name': []})

            self.book.loc[len(self.book)] = [
                id,
                url_id,
                title,
                en_title,
                price,
                ghat,
                discount,
                isbn,
                cover,
                page_count,
                solar_publish_year,
                ad_publish_year,
                edition,
                fastest_delivery,
                rate,
                available
            ]
            self.publisher.loc[len(self.publisher)] = [
                url_id,
                publisher
            ]
            for ou in outher:
                self.outher.loc[len(self.outher)] = [
                    url_id,
                    ou
                ]
            self.translator.loc[len(self.translator)] = [
                url_id,
                translator
            ]
            for aw in award:
                self.award.loc[len(self.award)] = [
                    url_id,
                    aw
                ]

            
    def crawl_books_tags(self, book_id, book_page):

        try:
            tags = book_page.select('.product-tags-item')
            if len(self.tag) == 100:
                self.tag_flag += 100
                print(self.tag_flag)
                self.save_all_csv('tag')
                self.tag = pd.DataFrame(columns=['book_id', 'tag_id', 'tag'])

            self.tag.loc[len(self.tag)] = [
                book_id,
                [re.findall(r'\d+', tag.get('href'))[0] for tag in tags],
                [i.text for i in tags]
            ]
        except:
            if len(self.tag) ==100 :
                self.tag_flag += 100
                print(self.tag_flag)
                self.save_all_csv('tag')
                self.tag = pd.DataFrame(columns=['book_id', 'tag_id', 'tag'])

            self.tag.loc[len(self.tag)] = [
                book_id,
                [None],
                [None]
            ]


if __name__ == '__main__':
    crawler = IranketabCrawler()
    crawler.start_crawling()
