# -*- coding: utf-8 -*-

from main import upload_file
from unicodedata import normalize
import pandas as pd
import os
os.chdir('../')

input_txt_file_path = u'/*_input/'
output_parq_path = '/output/data/parquet/'
s3_path = "DY/"
bucket_name = '*-warehouse'

if __name__ == "__main__":
    definition = (pd.read_csv("/*_definition.tsv",
                         delimiter='\t',
                         keep_default_na=False, encoding='utf-8') )
    # print(str.__contains__("사업체수.txt", '산업분류별(10차_대분류)_사업체수'))

    entries = os.listdir(input_txt_file_path)
    for enty in entries:    # File list
        col_list =[]
        data = pd.read_csv("/target/dy_input/" + normalize("NFC", enty),
                           sep="^",
                           encoding="utf-8")
        for index, row in definition.iterrows():
            if normalize("NFC", enty).__contains__(row['table_name_ko']):
                col_list.append(row['column_name_english'])
        if len(data.columns) == len(col_list):
            data.columns = col_list     # mapping column name
            data.to_parquet(output_parq_path + normalize("NFC", enty).replace("txt", "parquet")) 
            # print("ncp path : ", s3_path + row['table_name_en'] + "/" + normalize("NFC", enty).replace("txt", "parquet")) 

            upload_file(
                local_file_path=output_parq_path + str(normalize("NFC", enty).replace("txt", "parquet")),
                bucket_name=bucket_name,
                object_name=s3_path + str(row['table_name_en']) + "/" + str(normalize("NFC", enty).replace("txt", "parquet"))
            ) 
    exit(0)