from ugam_amazon_geo.config import *
from ugam_amazon_geo.pipelines import *
from ugam_amazon_geo.items import *
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from mymodules._common_ import c_replace
import re
import time

try:
    cursor = UgamAmazonGeoPipeline.cursor
    con = UgamAmazonGeoPipeline.con
    cookies = f"select Id,zipcode from cookies where Status='Pending'"
    # brand_select = f"select Id,searchterm from {db_brand_table} where Status = 'Pending'"

    cursor.execute(cookies)
    cookies = [column for column in cursor.fetchall()]
    for item in cookies:
        row_id=item[0]
        zipcode = item[1]

        dd = webdriver.ChromeOptions()
        dd = webdriver.Chrome(ChromeDriverManager().install())
        dd.get("https://www.amazon.com/")
        dd.maximize_window()
        time.sleep(2)
        dd.find_element_by_xpath('//a[@id="nav-global-location-popover-link"]').click()
        time.sleep(2)

        try:
            dd.find_element_by_id('GLUXZipUpdateInput').send_keys(f'{zipcode}')
            dd.find_element_by_xpath('//span[@id="GLUXZipUpdate"]').click()
            dd.find_element_by_xpath('//div[@class="a-popover-wrapper"]//div[@class="a-popover-footer"]//span[@class="a-declarative"]').click()
            dd.refresh()
            if zipcode in dd.page_source:

                time.sleep(2)
                cookieimid = ""
                d = dd.get_cookies()
                for c in d:
                    cookieimid += '{name}={value}; '.format(
                        name=c['name'],
                        value=c['value']
                    )

                update=f"UPDATE cookies SET cookies='{str(cookieimid)}',Status='Done' where Id={row_id}"
                print(update)
                cursor.execute(update)
                con.commit()
            else:
                print("------------ Zipcode Not Available ------------")

        except Exception as e:
            print(e)

except Exception as e:
    print(e)



