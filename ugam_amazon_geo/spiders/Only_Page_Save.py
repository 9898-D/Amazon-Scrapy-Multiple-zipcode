import hashlib
import time
import html
import datetime
import os
import scrapy
from ugam_amazon_geo.config import *
from ugam_amazon_geo.pipelines import *
from ugam_amazon_geo.items import *
from mymodules._common_ import c_replace
from scrapy.selector import Selector
import requests
import json
import selenium
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from mymodules._common_ import c_replace
import re

proxystorm={"https":"lum-customer-c_11e7173f-zone-zone_us:33p04eaqxtpu@zproxy.lum-superproxy.io:22225",
            "http":"lum-customer-c_11e7173f-zone-zone_us:33p04eaqxtpu@zproxy.lum-superproxy.io:22225"}
'your_apikey:'
proxy_host = "proxy.crawlera.com"
proxy_port = "8010"
proxy_auth = "your_apikey:"
proxies = {"https": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
      "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}



def unknownrepl(s):
    s = c_replace(s)
    a = s.encode('unicode_escape').decode('ascii')
    r = r'\\u[\w]{4}'
    c = c_replace(re.sub(r, '', a))
    c = c_replace(c)
    return c

handle_httpstatus_list = [404,403,503]

class Amazon_Geo(scrapy.Spider):

    name = 'Only_Page_Save'

    handle_httpstatus_list = [404,403,404,503,401]
    F_PATH = HTML

    def __init__(self, start='', end=''):

        try:
            self.cursor = UgamAmazonGeoPipeline.cursor
            self.con = UgamAmazonGeoPipeline.con
            self.start = start
            self.end = end
        except Exception as e:
            logger.error('exception in __init__ method main:{}'.format(e))

    def start_requests(self):

        try:
            self.cursor = UgamAmazonGeoPipeline.cursor
            self.con = UgamAmazonGeoPipeline.con
            brand_select = f"select Id,url,zipcode_input from {product_table} where Status = 'Pending' AND Id BETWEEN {self.start} AND {self.end}"
            # brand_select = f"select Id,searchterm from {db_brand_table} where Status = 'Pending'"

            self.cursor.execute(brand_select)
            brand_list = [column for column in self.cursor.fetchall()]


            for item in brand_list:
                row_id=item[0]
                url=item[1]
                zipcode = item[2]


                cookie = 'SELECT zipcode,cookies FROM cookies'
                self.cursor.execute(cookie)
                cookie = [column for column in self.cursor.fetchall()]

                for c in cookie:
                    zipcodec=c[0]
                    cookie=c[1]

                    if zipcode==zipcodec:

                        headers = {
                            "authority": "www.amazon.com",
                            # "method": "GET",
                            # 'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            "cookie": f'{cookie}',
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
                            # "user-agent": get_useragent(),
                            # "sec-fetch-site": "same-origin",
                            # "sec-ch-ua-platform": '"Windows"',
                            # 'scheme': 'https',
                            # 'X-Crawlera-Cookies' : "disable"
                        }

                        # headers={
                        #         'authority':'www.amazon.com',
                        #         'method':'GET',
                        #         'path':'/',
                        #         'scheme':'https',
                        #         'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                        #         'accept-language':'en-US,en;q=0.9',
                        #         'cookie':cookie,
                        #         'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
                        #         'X-Crawlera-Cookies': "disable"
                        # }
                        # headers['x-requested-with'] = "XMLHttpRequest"
                        # headers['X-Crawlera-Cookies'] = "disable"

                        # print(headers)
                        # mydata = json.loads(resp.text)
                        p_id = url.split('dp/')[1].split('/')[0]
                        if "?" in p_id:
                            p_id = p_id.split("?")
                            p_id = p_id[0]
                        else:
                            p_id = p_id

                        filename = f'/{row_id}_{p_id}_{zipcode}.html'
                        path = self.F_PATH + filename
                        path = path.replace("\\", "/")

                        time.sleep(1)
                        if os.path.exists(path):
                            yield scrapy.FormRequest(url=f'file:///{path}', callback=self.parse, dont_filter=True,
                                                     meta={'row_id': row_id, 'p_url': url, 'zpcode': zipcode})
                        else:
                            url1 = f'https://www.amazon.com/dp/{p_id}?th=1&psc=1'
                            import random
                            # zen_key=random.choice(zen_keys)
                            # print(zen_key)
                            # # pro_encodeurl = (urllib.parse.quote(product_url, safe=""))
                            # zen_response = f"https://app.zenscrape.com/api/v1/get?apikey={zen_key}&url="+ url1 +"&keep_headers=true&premium=true&country=us"
                            # scraper_key=['your_apikey']
                            # scraper_key=random.choice(scraper_key)
                            # # main_req = requests.get(f"http://api.scraperapi.com/?api_key=your_apikey&url={url1}&keep_headers=true&country_code=us",headers=headers)
                            # print(path)
                            # # if not os.path.exists(path):
                            # print(path)
                            # # if os.path.exists(path):
                            # #     yield scrapy.FormRequest(url=f'file:{path}', callback=self.parse,
                            # #                              dont_filter=True,
                            # #                              meta={'row_id': row_id, 'p_url': url, 'zpcode': zipcode})
                            # # else:
                            main_req = requests.get(f"http://api.scraperapi.com/?api_key=your_apikey&url={url1}&keep_headers=true&country_code=us",headers=headers)
                            # main_req = requests.get(url1,headers=headers,proxies=proxies,verify=False)
                            # main_req = requests.get(url1,headers=headers)
                            # print(main_req.text)
                            # with open("Hello.html",'w',encoding='utf-8')as f:
                            #     f.write(main_req.text)
                            # proxy_host = "proxy.crawlera.com"
                            # proxy_port = "8010"
                            # proxy_auth = "your_apikey:"
                            #
                            # crawleraProxies = {
                            #    "https": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
                            #    "http": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)
                            # }
                            # main_req=requests.get(url=url1,headers=headers,proxies=crawleraProxies,verify=False)
                            var_body = main_req.text
                            var_status_code = main_req.status_code
                            checking_var_response = Selector(text=var_body)
                            var_response_flag = False
                            var_zip_flag = False
                            # lower_case_var_resp = var_body.lower()
                            while var_response_flag == False:
                                if zipcodec in var_body:
                                    if 'Select your address' not in var_body:
                                        var_zip_flag = True

                                if (var_zip_flag and
                                    'Web Scraping API: Data Extraction at Scale & Without Getting Blocked' not in var_body
                                    and 'Sorry! Something went wrong' not in var_body
                                    and 'Type the characters you see in this image' not in var_body
                                    and 'An error has occured' not in var_body
                                    and 'zenscrape' not in var_body and 'Zenscrape' not in var_body
                                    and var_status_code == 200
                                    and not checking_var_response.xpath(
                                            "//html[contains(@class,'touch a-mobile')]")) \
                                        or (
                                        var_status_code == 404 or "We couldn't find that page" in var_body):
                                    var_response_flag = True
                                else:
                                    print("Wrong response Retrying after 2 sec..")
                                    time.sleep(2)
                                    # proxy_host = "proxy.crawlera.com"
                                    # proxy_port = "8010"
                                    # proxy_auth = "your_apikey:"
                                    #
                                    # crawleraProxies = {
                                    #     "https": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
                                    #     "http": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)
                                    # }
                                    # var_response = requests.get(url=url1, headers=headers, proxies=crawleraProxies,verify=False)
                                    var_response = requests.get(f"http://api.scraperapi.com/?api_key=your_apikey&url={url1}&keep_headers=true&country_code=us",headers=headers)
                                    # var_response = requests.get(zen_response,headers=headers)
                                    # var_response = requests.get(url1,headers=headers,proxies=proxies,verify=False)
                                    # var_response = requests.get(url1,headers=headers)
                                    # var_response = requests.get(url=url1, headers=headers, proxies=proxystorm, verify=False)
                                    var_body = var_response.text
                                    var_status_code = var_response.status_code
                                    checking_var_response = Selector(text=var_body)
                                try:
                                    # if not os.path.exists(path):
                                    with open(path, 'w', encoding='utf-8') as f:
                                        f.write(var_body)
                                        f.close()
                                except Exception as e:
                                    print("Error While Page Save", e)
                                    # config.error_fu(e)
                                    return None

                                path = path.replace("/", "\\")
                                path = path.replace("\\\\", "\\")
                            else:
                                pass

                        yield scrapy.FormRequest(url=f'file:{path}', callback=self.parse, headers=headers,dont_filter=True,meta={'row_id': row_id, 'p_url': url, 'zpcode': zipcode})

        except Exception as e:
            print(e)

    def parse(self, response, **kwargs):
        print(response.status)
        print("-------------- ONLY PAGE SAVE ----------------")

if __name__ == '__main__':
    from scrapy.cmdline import execute
    execute('scrapy crawl Only_Page_Save -a start=1 -a end=1'.split())