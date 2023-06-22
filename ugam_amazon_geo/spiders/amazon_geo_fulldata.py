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
from random_user_agent.params import SoftwareName, OperatingSystem
from random_user_agent.user_agent import UserAgent
from random import choice


def get_useragent():

    l1 = [SoftwareName.CHROME.value, SoftwareName.FIREFOX.value, SoftwareName.OPERA.value]
    software_names = [choice(l1)]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value, OperatingSystem.SUNOS.value]
    user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=1000)
    return user_agent_rotator.get_random_user_agent()

proxies={"https":"lum-customer-c_11e7173f-zone-zone_us:33p04eaqxtpu@zproxy.lum-superproxy.io:22225",
            "http":"lum-customer-c_11e7173f-zone-zone_us:33p04eaqxtpu@zproxy.lum-superproxy.io:22225"}
'2b900e10bfff42a7abdad3a43f18b01e:'
# proxy_host = "proxy.crawlera.com"
# proxy_port = "8010"
# proxy_auth = "295076252cfd4c6785db5845825ed279:"
# proxies = {"https": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
#       "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}



def unknownrepl(s):
    s = c_replace(s)
    a = s.encode('unicode_escape').decode('ascii')
    r = r'\\u[\w]{4}'
    c = c_replace(re.sub(r, '', a))
    c = c_replace(c)
    return c

handle_httpstatus_list = [404,403,503]

class Amazon_Geo(scrapy.Spider):

    name = 'AmazonGeo'

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
                            'X-Crawlera-Cookies' : "disable"
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
                            # # main_req = requests.get(f"http://api.scraperapi.com/?api_key=you_api_key&url={url1}&keep_headers=true&country_code=us",headers=headers)
                            # print(path)
                            # # if not os.path.exists(path):
                            # print(path)
                            # # if os.path.exists(path):
                            # #     yield scrapy.FormRequest(url=f'file:{path}', callback=self.parse,
                            # #                              dont_filter=True,
                            # #                              meta={'row_id': row_id, 'p_url': url, 'zpcode': zipcode})
                            # # else:
                            # main_req = requests.get(f"http://api.scraperapi.com/?api_key=959b3f5395cc4ae0255e17d7b4075c2e&url={url1}&keep_headers=true&country_code=us",headers=headers)
                            main_req = requests.get(url1,headers=headers,proxies=proxies,verify=False)
                            # main_req = requests.get(url1,headers=headers)
                            # print(main_req.text)
                            # with open("Hello.html",'w',encoding='utf-8')as f:
                            #     f.write(main_req.text)
                            proxy_host = "proxy.crawlera.com"
                            proxy_port = "8010"
                            proxy_auth = "your_apikey:"
                            if proxy_auth:
                                print("")
                            proxy_auth = "your_apikey:"


                            crawleraProxies = {
                               "https": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
                               "http": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)
                            }
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
                                    proxy_host = "proxy.crawlera.com"
                                    proxy_port = "8010"
                                    proxy_auth = "your_apikey:"
                                    # proxy_auth = "3b43493ce36f48a6977591dabc9de2d9:"

                                    crawleraProxies = {
                                        "https": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
                                        "http": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)
                                    }
                                    # var_response = requests.get(url=url1, headers=headers, proxies=crawleraProxies,verify=False)
                                    # var_response = requests.get(f"http://api.scraperapi.com/?api_key=b2860ce65d5a8848361f31f2ffe0300a&url={url1}&keep_headers=true&country_code=us",headers=headers)
                                    # var_response = requests.get(zen_response,headers=headers)
                                    var_response = requests.get(url1,headers=headers,proxies=proxies,verify=False)
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

                                    query=f"update {product_table} SET page_save_status='Done'"
                                    self.cursor.execute(query)
                                    self.con.commit()
                                except Exception as e:
                                    print("Error While Page Save", e)
                                    # config.error_fu(e)
                                    return None

                                path = path.replace("/", "\\")
                                path = path.replace("\\\\", "\\")
                            else:
                                pass

                            yield scrapy.FormRequest(url=f'file:{path}', callback=self.parse,dont_filter=True,meta={'row_id': row_id, 'p_url': url, 'zpcode': zipcode})

        except Exception as e:
            print(e)

    def parse(self, response, **kwargs):

        print(response.status)
        row_id = response.meta.get('row_id')
        p_url = response.meta.get('p_url')
        zipcode = response.meta.get('zpcode')

        # if "Sorry! We couldn't find that page. Try searching or go to Amazon's home page." or "Amazon.com Page Not Found" not in response.text:
        if "Sorry! We couldn't find that page. Try searching or go to Amazon's home page." not in response.text and "Amazon.com Page Not Found" not in response.text:

            p_id = p_url.split('dp/')[1].split('/')[0]
            if "?" in p_id:
                p_id = p_id.split("?")
                p_id = p_id[0]
            else:
                p_id = p_id
            filename = f'/{row_id}_{p_id}_{zipcode}.html'
            path = self.F_PATH + filename
            path = path.replace("\\", "/")

            # if zipcode in response.text:
            #     if os.path.exists(path):
            #         pass
            #     else:
            # time.sleep(2)
            # with open(path, 'w', encoding='utf-8')as f:
            #     time.sleep(1)
            #     f.write(response.text)

            # else:
            #     print("-------- ZIPCODE NOT IN PAGE ----------")
            #     return  None

            # TODO -------  ITEMS CALL --------------

            item = UgamAmazonGeoItem()
            item['Id'] = row_id

            # TODO ----------- HEADERS SCRAPE ------------------

            # # TODO ----------- CATEGORY BREADCUMBS ------------

            try:
                category = response.xpath('//div[@id="wayfinding-breadcrumbs_container"]//ul/li/span[@class="a-list-item"]//text()').getall()


                if category==[]:

                    item['category_path']="n/a"
                else:
                    category=c_replace(category)
                    bread = '#||#'.join(category)
                    nw_bread = c_replace(bread).replace('"','')
                    print(nw_bread)
                    item['category_path'] = c_replace(nw_bread)
            except Exception as e:
                print(e)

            # TODO ----------- Product Image ------------

            try:
                prd_img_li=[]
                prd_img = response.xpath('//*[@id="altImages"]//*[@class="a-button-text"]//@src').getall()
                if not prd_img:
                    prd_img=response.xpath('//div[@id="imageBlockThumbs"]//div//img//@src').getall()
                    if not prd_img:
                        prd_img=response.xpath('//ul[@class="a-unordered-list a-nostyle a-button-list a-vertical regularAltImageViewLayout"]//span//img/@src').getall()
                        if not prd_img:
                            prd_img=response.xpath('//div[@id="ebooks-main-image-container"]//img//@src').getall()

                if prd_img!=[]:
                    prd_img.pop()
                    for img in prd_img:
                        img = str(img)
                        prd_img_li.append(img.replace('40_.jpg', '350_.jpg').replace('50_.jpg', '350_.jpg'))

                    if prd_img!=[]:
                        nw_al_prd_img=' #||# '.join(prd_img_li)
                        print(nw_al_prd_img)
                        item['Product_image'] = c_replace(str(nw_al_prd_img))
                    else:
                        item['Product_image'] ="n/a"
                else:
                    item['Product_image'] = "n/a"
            except Exception as e:
                print(e)
            #
            # # TODO ----------- Product Name ----------------

            try:
                prd_name = response.xpath('//h1/span/text()').get()
                nw_prd_name = c_replace(prd_name)
                nw_prd_nm=nw_prd_name.replace('"',"'")
                print(nw_prd_nm)
                item['Product_Name']=unknownrepl(nw_prd_nm)
            except Exception as e:
                print(e)

            # TODO ----------- MARKDOWN PRICE -----------------

            try:
                mrk_price = response.xpath('//div[@id="apex_desktop"]//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]//text()').get()
                if mrk_price==None:
                    mrk_price=response.xpath('//div[@id="apex_desktop"]//span[@class="a-price a-text-price"]//span//text()').get()

                    if mrk_price==None:
                        mrk_price=response.xpath('//div[@id="apex_desktop"]//span[@class="a-price a-text-price a-size-medium apexPriceToPay"]//span//text()').get()
                        if mrk_price:
                            nw_mrk_price = c_replace(mrk_price).replace('"', '')
                            print(nw_mrk_price)
                        else:
                            nw_mrk_price="n/a"
                    else:
                        if mrk_price:
                            nw_mrk_price = c_replace(mrk_price).replace('"', '')
                            print(nw_mrk_price)
                        else:
                            nw_mrk_price="n/a"
                else:
                    if mrk_price:
                        nw_mrk_price = c_replace(mrk_price).replace('"', '')
                        print(nw_mrk_price)
                    else:
                        nw_mrk_price="n/a"
            except Exception as e:
                print(e)
                nw_mrk_price = "n/a"

            # TODO ----------- Regular Price ------------
            try:
                regu_price = response.xpath('//div[@id="apex_desktop"]//*[contains(text(),"M.R.P.: ")]//span//span//text()').get()
                if regu_price == None:
                    regu_price=response.xpath('//div[@id="apex_desktop"]//*[contains(text(),"M.R.P.: ")]//..//span[@class="a-price a-text-price a-size-base"]//span//text()').get()
                    if regu_price==None:
                        regu_price=response.xpath('//div[@id="apex_desktop"]//*[contains(text(),"M.R.P.: ")]//span//span//text()').get()
                        if regu_price:
                            nw_regu_price = c_replace(regu_price)
                            print(regu_price)
                        else:
                            nw_regu_price = "n/a"
                    else:
                        nw_regu_price = c_replace(regu_price)
                        print(regu_price)
                else:
                    nw_regu_price = c_replace(regu_price)
                    print(regu_price)
            except Exception as e:
                print(e)
                nw_regu_price = "n/a"


            # TODO ----------- Product_ID  ------------
            try:
                product_id = p_url.split('dp/')[1].split('/')[0]

                if "?" in product_id:
                    product_id = product_id.split("?")
                    product_id = product_id[0]
                else:
                    product_id = product_id

                item['product_id'] = c_replace(product_id)
            except Exception as e:
                print(e)

            # TODO ----------- Manufacture_PART_NUMBER  ------------

            try:
                part_num = response.xpath('//*[contains(text(),"Part Number")]/following-sibling::td//text()').get()
                if part_num == None:
                    nw_part_num = "n/a"
                else:
                    nw_part_num = c_replace(part_num).replace('"','')

            except Exception as e:
                print(e)
                nw_part_num = "n/a"

            # TODO ----------- Brand Name  ------------

            try:
                brandname = response.xpath('//table[@id="productDetails_techSpec_section_1"]//*[contains(text(),"Brand")]//..//td//text()').get()

                if not brandname:
                    brandname1=response.xpath('//table[@class="a-normal a-spacing-micro"]//*[contains(text(),"Brand")]//../following-sibling::td/span/text()').get()
                    if not brandname1:
                        brandname2=response.xpath('//table[@id="product-specification-table"]//*//*[contains(text(),"Brand")]//..//td').get()
                        if not brandname2:
                            brandname3=response.xpath('//div[@class="a-section a-spacing-none"]/a[@id="bylineInfo"]//text()').get()
                            if brandname3:
                                if 'Brand' in brandname3:
                                    brandname3=brandname3.split(':')[-1]
                                    brandname3 = c_replace(brandname3)
                                    nw_brandname = brandname3
                                else:
                                    nw_brandname='n/a'
                            else:
                                nw_brandname="n/a"
                        else:
                            if brandname2:
                                brandname2=c_replace(brandname2)
                                nw_brandname=brandname2
                            else:
                                nw_brandname="n/a"

                    else:
                        if brandname1:
                            nw_brandname = c_replace(brandname1).replace('"','')
                            print(nw_brandname)
                        else:
                            nw_brandname="n/a"
                else:
                    if brandname:
                        nw_brandname = c_replace(brandname).replace('"','')
                        print(nw_brandname)
                    else:
                        nw_brandname = "n/a"

            except Exception as e:
                print(e)
                nw_brandname = "n/a"


            # TODO ----------- Colour Finish  ------------
            try:
                colour_finsh1=response.xpath('//table[@id="productDetails_techSpec_section_1"]//*[contains(text(),"Color")]//..//td//text()').get()
                if not colour_finsh1:
                    colour_finsh2 = response.xpath('//table[@id="product-specification-table"]//*[contains(text(),"Color")]//..//td//text()').get()
                    if colour_finsh2:
                        mn_colour = c_replace(colour_finsh2).replace('"', '')
                    else:
                        colour_finsh3 = response.xpath('//table[@id="productDetails_detailBullets_sections1"]//*[contains(text(),"Color")]//..//td//text()').get()
                        if colour_finsh3:
                            mn_colour = c_replace(colour_finsh3).replace('"', '')
                        else:
                            # colour_finsh4 = response.xpath('//table[@class="a-normal a-spacing-micro"]//*[contains(text(),"Color")]//../following-sibling::td/span/text()').get()
                            colour_finsh4 = response.xpath('//table[@class="a-normal a-spacing-micro"]//*[contains(text(),"Color")]//text()').getall()
                            if "Color Temperature" in colour_finsh4 or colour_finsh4==[]:
                                mn_colour = "n/a"
                            else:
                                colour_finsh4 = response.xpath('//table[@class="a-normal a-spacing-micro"]//*[contains(text(),"Color")]//../following-sibling::td/span/text()').get()
                                mn_colour = c_replace(colour_finsh4).replace('"', '')
                else:
                    if "Kelvin" in colour_finsh1:
                        mn_colour = "n/a"
                    else:
                        print(len(colour_finsh1[0]))
                        mn_colour = c_replace(colour_finsh1).replace('"', '')

            except Exception as e:
                print(e)
                mn_colour="n/a"

            # TODO ----------- Capacity  ------------

            try:
                key=response.xpath('//table[@id="product-specification-table"]//tr//th//text()').getall()
                val=response.xpath('//table[@id="product-specification-table"]//tr//td//text()').getall()

                if key==[]:
                    key=response.xpath('//table[@id="productDetails_techSpec_section_1"]//tr//th//text()').getall()
                    val=response.xpath('//table[@id="productDetails_techSpec_section_1"]//tr//td//text()').getall()

                    if key==[]:
                        nw_cap="n/a"
                    else:
                        cap = {}
                        for i, j in zip(key,val):
                            cap[i] = j
                        print(cap)
                        nw_cap = ''
                        for x, l in cap.items():
                            if 'Capacity' in x:
                                nw_cap = nw_cap + l
                                break
                        print(nw_cap)
                        if nw_cap=='':
                            nw_cap='n/a'
                        else:
                            nw_cap=nw_cap
                else:
                    cap={}
                    for i,j in zip(key,val):
                        cap[i]=j
                    print(cap)
                    nw_cap=''
                    for x,l in cap.items():
                        if 'Capacity' in x:
                            nw_cap=nw_cap+l
                            break
                    print(nw_cap)
                    if nw_cap=='':
                        nw_cap="n/a"
                    else:
                        nw_cap=nw_cap
            except Exception as e:
                print(e)
                nw_cap = "n/a"

            # TODO --------------- Installation_Type  -------------------

            try:
                keys = response.xpath('//table[@id="productDetails_techSpec_section_1"]//tr//th//text()').getall()
                vals = response.xpath('//table[@id="productDetails_techSpec_section_1"]//tr//td//text()').getall()

                if keys == []:
                    nw_install1 = "n/a"
                else:
                    cap = {}
                    for i, j in zip(keys, vals):
                        cap[i] = j
                    print(cap)
                    nw_install1 = ''
                    for x, l in cap.items():
                        if 'Installation' in x:
                            nw_install1 = nw_install1 + l
                            break
                    print(nw_install1)
                    if nw_install1 == '':
                        nw_install1 = 'n/a'
                    else:
                        nw_install1 = nw_install1
            except Exception as e:
                print(e)
                nw_install1 = "n/a"

            # TODO ----------- Product Description  ------------
            try:
                prod_desc = response.xpath('//*[contains(text(),"Product Description")]//..//..//div[@id="productDescription"]//p//span//text()').getall()

                if prod_desc == [] or prod_desc==[' '] or prod_desc[0]=="Specs:" or prod_desc==" ":
                    prod_desc = response.xpath('//*[contains(text(),"Product Description")]//..//div[@class="a-section launchpad-text-left-justify"]/p//text()').getall()

                    if prod_desc==[] or prod_desc==[' '] or prod_desc[0]=="Specs:" or prod_desc==" ":
                        prod_desc=response.xpath('//*[contains(text(),"Product Description")]//..//..//p[@class="a-spacing-base"]//text()').getall()

                        if prod_desc==[] or prod_desc==[' '] or prod_desc[0]=="Specs:" or prod_desc==" ":
                            prod_desc=response.xpath('//*[contains(text(),"Product Description")]//..//div[@class="apm-sidemodule-textright"]//p//text()').getall()

                            if prod_desc==[] or prod_desc==[' '] or prod_desc[0]=="Specs:" or prod_desc==" ":

                                prod_desc=response.xpath('//*[contains(text(),"Specs:")]//../following-sibling::ul/li/span//text()').getall()

                                if prod_desc == [] or prod_desc == [' '] or prod_desc[0] == "Specs:" or prod_desc==" ":
                                    prod_desc=response.xpath('//*[contains(text(),"Product Description")]//../..//p/span/text()').getall()

                                    if prod_desc==[] or prod_desc==[' '] or prod_desc[0]=="Specs:" or prod_desc==" ":
                                        prod_desc=response.xpath('//div[@class="apm-sidemodule-textright"]//span//text()').getall()

                                        if prod_desc==[] or prod_desc==[' '] or prod_desc[0]=="Specs:" or prod_desc==" ":
                                            prod_desc=response.xpath('//div[@class="celwidget aplus-module module-2 aplus-standard"]//div[@class="apm-sidemodule-textright"]//text()').getall()

                                            if prod_desc==[] or prod_desc==[' '] or prod_desc[0]=="Specs:" or prod_desc==" ":
                                                nw_prod_desc="n/a"
                                            else:
                                                print(prod_desc)
                                                j_prod_desc = ''.join(prod_desc)
                                                nw_prod_desc = c_replace(j_prod_desc.replace('"', ''))
                                                print(nw_prod_desc)
                                        else:
                                            print(prod_desc)
                                            j_prod_desc = ''.join(prod_desc)
                                            nw_prod_desc = c_replace(j_prod_desc.replace('"', ''))
                                            print(nw_prod_desc)
                                    else:
                                        j_prod_desc = ''.join(prod_desc)
                                        nw_prod_desc = c_replace(j_prod_desc.replace('"', ''))
                                        print(nw_prod_desc)
                                else:
                                    j_prod_desc = ''.join(prod_desc)
                                    nw_prod_desc = c_replace(j_prod_desc.replace('"', ''))
                                    print(nw_prod_desc)
                            else:
                                j_prod_desc = ''.join(prod_desc)
                                nw_prod_desc = c_replace(j_prod_desc.replace('"', ''))
                                print(nw_prod_desc)
                        else:
                            j_prod_desc = ''.join(prod_desc)
                            # nw_prod_desc=j_prod_desc.replace('💎','')
                            nw_prod_desc = c_replace(j_prod_desc.replace('"',''))
                            print(nw_prod_desc)
                    else:
                        j_prod_desc = ''.join(prod_desc)
                        nw_prod_desc = c_replace(j_prod_desc.replace('"', ''))
                        print(nw_prod_desc)
                else:
                    if prod_desc:
                        j_prod_desc = ''.join(prod_desc)
                        nw_prod_desc = c_replace(j_prod_desc.replace('"',''))
                        print(nw_prod_desc)
                    else:
                        nw_prod_desc = "n/a"
            except Exception as e:
                print(e)
                nw_prod_desc = "n/a"

            item['htmlpath'] = path
            item['extraction_date'] = datetime.today().strftime('%d/%m/%Y %H:%M:%S %p')
            item['Markdown_Price'] = c_replace(nw_mrk_price)
            item['regular_price'] = c_replace(nw_regu_price)
            item['Manufacturer_Part_Number'] = c_replace(nw_part_num)
            item['Brand_Name'] = c_replace(nw_brandname)
            item['Color_Finish'] = c_replace(mn_colour)
            item['Capacity'] = c_replace(nw_cap)
            item['Installation_type'] = c_replace(nw_install1)
            item['Detailed_Specification'] = "n/a"
            item['Product_Description'] = c_replace(nw_prod_desc)
            item['zipcode_site'] = "n/a"
            item['Status']="Done"

            yield item
 
        else:

            p_id = p_url.split('dp/')[1].split('/')[0]
            if "?" in p_id:
                p_id=p_id.split("?")
                p_id=p_id[0]
            else:
                p_id=p_id

            filename = f'/{row_id}_{p_id}_{zipcode}.html'
            path = self.F_PATH + filename
            path = path.replace("\\", "/")

            if os.path.exists(path):
                pass
            else:
                try:
                    with open(path, 'w', encoding='utf-8')as f:
                        f.write(response.text)
                except Exception as e:
                    print(e)

            item=UgamAmazonGeoItem()
            item['Id']=row_id
            item['htmlpath'] = path
            item['extraction_date'] = datetime.today().strftime('%d/%m/%Y %H:%M:%S %p')
            item['Product_image'] = "n/a"
            item['category_path'] = "n/a"
            item['Product_Name'] = "n/a"
            item['Markdown_Price'] = "n/a"
            item['regular_price'] = "n/a"
            item['product_id'] = "n/a"
            item['Manufacturer_Part_Number'] = "n/a"
            item['Brand_Name'] = "n/a"
            item['Color_Finish'] = "n/a"
            item['Capacity'] = "n/a"
            item['Installation_type'] = "n/a"
            item['Detailed_Specification'] = "n/a"
            item['Product_Description'] = "n/a"
            item['zipcode_site']="n/a"
            item['Status'] = "Not Found"


            yield item
    #   todo CREATE CSV FILE WHEN UNCOMMENTS THIS CODE WHEN COUNT IS 0----------------------------------------------------

    # def close(spider, reason):
    #     try:
    #         con = UgamAmazonGeoPipeline.con
    #         import pandas as pd
    #         sql_query = pd.read_sql_query(f'SELECT `uniqueIdentifier`,`extraction_date`,`category_path`,`Product_image`,`url`,`Product_Name`,`Markdown_Price`,`regular_price`,`product_id`,`Manufacturer_Part_Number`,`Brand_Name`,`Color_Finish`,`Capacity`,`Installation_type`,`Detailed_Specification`,`Product_Description`,`zipcode_input` from {product_table} where Status="Done"' , con)
    #         df = pd.DataFrame(sql_query)
    #         df.to_csv(f'GEO_PDP_amazon_US_na_output_{today_date}.csv', encoding='utf-8-sig', index=False)
    #         print("---------------------**********************************-------------------------------")
    #         print("create File")
    # 
    #     except Exception as e:
    #         print(e)

    # Todo ------------ GENERATE EXCEL FILE ----------
    # def close(spider, reason):
    #     try:
    #         con = UgamAmazonGeoPipeline.con
    #         import pandas as pd
    #         sql_query = pd.read_sql_query(f'SELECT `uniqueIdentifier`,`extraction_date`,`category_path`,`Product_image`,`url`,`Product_Name`,`Markdown_Price`,`regular_price`,`product_id`,`Manufacturer_Part_Number`,`Brand_Name`,`Color_Finish`,`Capacity`,`Installation_type`,`Detailed_Specification`,`Product_Description`,`zipcode_input` from {product_table} where Status="Done"' , con)
    #         df = pd.DataFrame(sql_query)
    #         # df.to_excel(f'GEO_PDP_amazon_US_na_output_{today_date}.xlsx', encoding='utf-8-sig', index=False)
    #         df.to_excel(f'GEO_PDP_amazon_US_na_output_{today_date}.xlsx', index=False, encoding='utf-8-sig')
    #         print("---------------------**********************************-------------------------------")
    #         print("create File")
    #
    #     except Exception as e:
    #         print(e)

    #  Todo ------------ GENERATE EXCEL FILE NEW----------------------------
    # def close(spider, reason):
    #     try:
    #         con = UgamAmazonGeoPipeline.con
    #         cursor = UgamAmazonGeoPipeline.cursor
    #         import pandas as pd
    #         query_pen = f"Select Id from {product_table} where status='Pending'"  # Todo For Pending Query
    #         cursor.execute(query_pen)
    #         colum = cursor.fetchall()
    #
    #         if colum == ():
    #             sql_query = pd.read_sql_query(f'SELECT `uniqueIdentifier`,`extraction_date`,`category_path`,`Product_image`,`url`,`Product_Name`,`Markdown_Price`,`regular_price`,`product_id`,`Manufacturer_Part_Number`,`Brand_Name`,`Color_Finish`,`Capacity`,`Installation_type`,`Detailed_Specification`,`Product_Description`,`zipcode_input` from {product_table} where Status="Done"',con)
    #             df = pd.DataFrame(sql_query)
    #             path = 'D:\\Dhruv_Choubisa_Amazon_US_GEO\\ugam_amazon_geo\\OUTPUT'
    #             file_pth = path + f'\\{today_date}'
    #
    #             if not os.path.exists(file_pth):
    #                 os.mkdir(file_pth)
    #             else:
    #                 pass
    #
    #             file_name = "GEO_PDP_amazon_US_na_output"
    #             file_nm = f'{file_pth}\\{file_name}_{today_date}.xlsx'
    #             print(file_nm)
    #
    #             df.to_excel(file_nm, index=False)
    #             print("---------------------**********************************-------------------------------")
    #             print("------------------------ CSV FILE IS CREATED ENJOY! -------------------------------")
    #         else:
    #             print(
    #                 f"--------------------------- PLEASE COMPLETE ALL PENDING COUNTS IN THIS TABLE {product_table} --------------------------")
    #
    #     except Exception as e:
    #         print(e)

    #  Todo ------------ GENERATE CSV FILE NEW----------------------------
    def close(spider, reason):
        try:
            con = UgamAmazonGeoPipeline.con
            cursor = UgamAmazonGeoPipeline.cursor
            import pandas as pd
            query_pen = f"Select Id from {product_table} where status='Pending'"  # Todo For Pending Query
            cursor.execute(query_pen)
            colum = cursor.fetchall()

            if colum == ():
                sql_query = pd.read_sql_query(f'SELECT `uniqueIdentifier`,`extraction_date`,`category_path`,`Product_image`,`url`,`Product_Name`,`Markdown_Price`,`regular_price`,`product_id`,`Manufacturer_Part_Number`,`Brand_Name`,`Color_Finish`,`Capacity`,`Installation_type`,`Detailed_Specification`,`Product_Description`,`zipcode_input` from {product_table} where Status="Done"' ,con)
                df = pd.DataFrame(sql_query)
                path = 'D:\\Dhruv_Choubisa_Amazon_US_GEO\\ugam_amazon_geo\\OUTPUT'
                file_pth = path + f'\\{today_date}'

                if not os.path.exists(file_pth):
                    os.mkdir(file_pth)
                else:
                    pass

                file_name = "GEO_PDP_amazon_US_na_output"
                file_nm = f'{file_pth}\\{file_name}_{today_date}.csv'
                print(file_nm)

                df.to_csv(file_nm, encoding='utf-8-sig', index=False)
                print("---------------------**********************************-------------------------------")
                print("------------------------ CSV FILE IS CREATED ENJOY! -------------------------------")
            else:
                print(
                    f"--------------------------- PLEASE COMPLETE ALL PENDING COUNTS IN THIS TABLE {product_table} --------------------------")

        except Exception as e:
            print(e)



    # def close(spider, reason):
    #     try:
    #         con = UgamAmazonGeoPipeline.con
    #         cursor = UgamAmazonGeoPipeline.cursor
    #         import pandas as pd
    #         query_pen = f"Select Id from {product_table} where status='Pending'"  # Todo For Pending Query
    #         cursor.execute(query_pen)
    #         colum = cursor.fetchall()
    #
    #         sql_query = pd.read_sql_query(
    #             f'SELECT `uniqueIdentifier`,`extraction_date`,`category_path`,`Product_image`,`url`,`Product_Name`,`Markdown_Price`,`regular_price`,`product_id`,`Manufacturer_Part_Number`,`Brand_Name`,`Color_Finish`,`Capacity`,`Installation_type`,`Detailed_Specification`,`Product_Description`,`zipcode_input` from {product_table} where Status="Done"',
    #             con)
    #         df = pd.DataFrame(sql_query)
    #         path = 'D:\\Dhruv_Choubisa_Amazon_US_GEO\\ugam_amazon_geo\\OUTPUT'
    #         file_pth = path + f'\\{today_date}'
    #
    #         if not os.path.exists(file_pth):
    #             os.mkdir(file_pth)
    #         else:
    #             pass
    #
    #         file_name = "GEO_PDP_amazon_US_na_output"
    #         file_nm = f'{file_pth}\\{file_name}_{today_date}.csv'
    #         print(file_nm)
    #
    #         df.to_csv(file_nm, encoding='utf-8-sig', index=False)
    #         print("---------------------**********************************-------------------------------")
    #         print("------------------------ CSV FILE IS CREATED ENJOY! -------------------------------")
    #
    #     except Exception as e:
    #         print(e)

if __name__ == '__main__':
    from scrapy.cmdline import execute
    execute('scrapy crawl AmazonGeo -a start=1 -a end=1'.split())

