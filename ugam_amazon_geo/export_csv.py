# """coding is export tsv form export tsv referen in dowload file export tsv.py
# QA file
# """
# from datetime import datetime
# import csv
# from ugam_amazon_geo.pipelines import *
# import datetime
# import os
# import shutil
# import time
# import pymysql
# import re
# import pandas as pd
# from numpy.ma import count
# from ugam_amazon_geo.config import *
# # from common_code_100_site_project import ugam_3_Category_PDP
# # from worlddutyfree_menu.unique_process import
#
# # check directory exists or not and make csv file in the bellow directory
# class Export_Csv:
#     def __init__(self):
#         try:
#             Csv_Output_File_Path= "D:\\Daily Scheduler\\0. UGAM\\0. CSV\\Amazon_Geo\\" + time.strftime('%Y') + "\\" + time.strftime("%m") + "\\" + time.strftime("%d")
#             self.ftpCsv_Output_File_Path = "\\\\192.168.1.133\\d\\Ugam-Project\\UGAM-Au-Category_Menu\\Output\\" + time.strftime('%Y') + "\\" + time.strftime("%m") + "\\" + time.strftime("%d") + "\\" + "Delivered Data"
#             self.Csv_Output_File_Csv = Csv_Output_File_Path + "\\CSV"
#
#             try:
#
#                 if not os.path.exists(Csv_Output_File_Path):
#                     os.makedirs(Csv_Output_File_Path)
#                 if not os.path.exists(self.Csv_Output_File_Csv):
#                     os.makedirs(self.Csv_Output_File_Csv)
#                 if not os.path.exists(self.ftpCsv_Output_File_Path):
#                     os.makedirs(self.ftpCsv_Output_File_Path)
#
#             except Exception as e:
#                 logger.error("make dir in constructor method error:{}".format(e))
#         except Exception as e:
#             logger.error("main constructor in export csv class error:{}".format(e))
#
#     """ ---export csv method--- """
#     def export_csv(self):
#         try:
#             conn = pymysql.connect(host=db_host, user=db_user, password=db_passwd, database=db_name, use_unicode=True, charset="utf8")
#             cursor = conn.cursor()
#
#             today_date = datetime.now().strftime('_%d_%m_%Y')
#             # linkdata_table_name = db_data_table + today_date
#
#             # select link data table sequence of headers
#             try:
#                 cursor.execute(f"""select Id from {product_table} where Status!='Pending'""")
#                 rows = cursor.fetchall()
#                 print(len(rows))
#                 fetch_row_count = int(count(rows))
#                 # print(fetch_row_count)
#                 print('select table :', product_table)
#
#                 select_sql_query = f"""select `Id`,`htmlpath`,`uniqueIdentifier`,`extraction_date`,`category_path`,`Product_image`,`Product_Name`,`Markdown_Price`,`regular_price`,`url`,`product_id`,`Manufacturer_Part_Number`,`Brand_Name`,`Color_Finish`,`Capacity`,`Installation_type`,`Detailed_Specification`,`zipcode_site`,`Product_Description`,`zipcode_input` from {product_table} where Status='Done'"""
#
#                 data_frame = pd.read_sql(select_sql_query, conn)
#                 # select_sql_query = f"""SELECT `Date of Crawl`, Retailer, Brand,`Product ID`,`Product Title`,`Product URL`,`Image URL`,MPN,`List Price`,`Markdown Price`,`Seller Name`,`Stock Status`,`Product Description`,`Product Specification`,`Product Features`,`Lead time`, category_url FROM {db_data_table} where Status<>'Pending'"""
#
#                 # cursor.execute(select_sql_query)
#                 # conn.commit()
#                 # print(select_sql_query)
#
#                 data_frame = pd.read_sql(select_sql_query, conn)
#
#                 # csv_file_date = datetime.now().strftime("%Y-%m-%d")
#
#                 csv_file_date = datetime.now().strftime("%m%d%y")
#
#                 save_in_dir = self.Csv_Output_File_Csv
#
#                 # tsv_filename = save_in_dir + f"\\PDP_amazon_fr_NA_output_101820.tsv"
#                 csv_filename = save_in_dir + f"\\Amazon_Geo{csv_file_date}.csv"
#                 ftpcsv_filename = self.ftpCsv_Output_File_Path + f"\\Amazon_Geo_output{csv_file_date}.csv"
#                 if os.path.exists(csv_filename):
#                     os.remove(csv_filename)
#                 data_frame.to_csv(csv_filename, index=False, encoding='utf-8-sig')
#                 data_frame.to_csv(ftpcsv_filename, index=False, encoding='utf-8-sig')
#
#                 tsv_filename = save_in_dir + f"\\Amazon_Geo_output{csv_file_date}.tsv"
#                 ftptsv_filename = self.ftpCsv_Output_File_Path + f"\\Amazon_Geo_output{csv_file_date}.tsv"
#                 if os.path.exists(tsv_filename):
#                     os.remove(tsv_filename)
#
#                 data_frame.to_csv(tsv_filename, index=False, sep='\t', encoding='utf-8-sig')
#                 data_frame.to_csv(ftptsv_filename, index=False, sep='\t', encoding='utf-8-sig')
#
#                 with open(file=csv_filename, mode='r', encoding='utf8') as f:
#                     csv_reader = csv.reader(f, delimiter=",")
#                     data = list(csv_reader)
#                     row_count = len(data)
#                 # data_frame.to_csv(csv_filename, index=False, encoding='utf-8-sig')
#
#                     # print('row_count:', row_count - 1)
#                 if fetch_row_count == row_count - 1:
#                     print("Both Row Checked !")
#                     print("Successfully..... ")
#                 else:
#                     os.remove(csv_filename)
#                     print('File Recreating...')
#                     Export_Csv.export_csv(self)
#                 if os.path.exists(csv_filename):
#                     src = os.path.realpath(csv_filename)
#                     zip_filename = save_in_dir + f"\\Amazon_Geo_output{csv_file_date}"
#                     zip_filename_2 = save_in_dir + f"\\Amazon_Geo_utput{csv_file_date}.zip"
#                     if os.path.exists(zip_filename_2):
#                         os.remove(zip_filename_2)
#                     zip_output_path_csv, tail = os.path.split(src)
#                     shutil.make_archive(base_name=zip_filename, format='zip',
#                                         root_dir=zip_output_path_csv, base_dir=tail)
#                 print("Successfully Done............!")
#                 print("Going for QA............!")
#                 # from ldlc_fr_category.QA import QA
#                 # QA()
#                 print("QA Done............!")
#
#
#
#             except Exception as e:
#                 logger.error('export csv method link data table select error:{}'.format(e))
#
#         except Exception as e:
#             logger.error("export csv method sql connection error:{}".format(e))
#
# c = Export_Csv()
# c.export_csv()