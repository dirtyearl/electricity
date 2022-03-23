#!/home/azureuser/miniconda3/envs/electricity/bin/python

import os, requests, time, random, zipfile, shutil
from azure.storage.blob import BlobServiceClient
import pandas as pd

excel_names = dict()
excel_names.update({'2008':'eia923December2008.xls'})
excel_names.update({'2009':'EIA923 SCHEDULES 2_3_4_5 M Final 2009 REVISED 05252011.XLS'})
excel_names.update({'2012':'EIA923_Schedules_2_3_4_5_M_12_2012_Final_Revision.xlsx'})
excel_names.update({'2014':'EIA923_Schedules_2_3_4_5_M_12_2014_Final_Revision.xlsx'})
excel_names.update({'2015':'EIA923_Schedules_2_3_4_5_M_12_2015_Final_Revision.xlsx'})
excel_names.update({'2016':'EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx'})
excel_names.update({'2017':'EIA923_Schedules_2_3_4_5_M_12_2017_Final_Revision.xlsx'})
excel_names.update({'2018':'EIA923_Schedules_2_3_4_5_M_12_2018_Final_Revision.xlsx'})
excel_names.update({'2019':'EIA923_Schedules_2_3_4_5_M_12_2019_Final_Revision.xlsx'})
excel_names.update({'2020':'EIA923_Schedules_2_3_4_5_M_12_2020_Final_Revision.xlsx'})
excel_names.update({'2021':'EIA923_Schedules_2_3_4_5_M_12_2021_18FEB2022.xlsx'})
excel_names.update({'2013':'EIA923_Schedules_2_3_4_5_2013_Final_Revision.xlsx'})
excel_names.update({'2011':'EIA923_Schedules_2_3_4_5_2011_Final_Revision.xlsx'})
excel_names.update({'2010':'EIA923 SCHEDULES 2_3_4_5 Final 2010.xls'})

def retrieve_file(dl_path, ul_path):
    try:
        resp = requests.get(dl_path, allow_redirects=False)
        with open(ul_path, 'wb') as obj:
            obj.write(resp.content)
        return None
    except Exception as ex:
        print(f"Exception: {ex}")


def unzip_file(local_file_name):
    with zipfile.ZipFile(local_file_name, 'r') as zfile:
        zfile.extractall('data/tmp')

def write_csv(year, write_name):
    header = 5 if year >= 2011 else 7
    pd.read_excel(os.path.join('data/tmp', excel_names[str(year)]),
                  sheet_name=0, header=header).to_csv(f'data/{write_name}', index=False)

def upload_file(container_name, local_file_name, upload_file_path,
        connect_str='AZURE_STORAGE_CONNECTION_STRING', overwrite=False):
    if connect_str:
        connect_str = os.getenv(connect_str)
    else:
        raise Exception("Connection string not defined")

    try:
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=local_file_name)

        # Upload the created file
        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=overwrite)

    except Exception as ex:
        print('Exception:')
        print(ex)

if __name__ == "__main__":
    os.environ['AZURE_STORAGE_CONNECTION_STRING'] = "DefaultEndpointsProtocol=https;AccountName=dataingest002581;AccountKey=Zuz8Bx41arXKz8vSzuB+d5qcDoL40bqpu5VqTASDTUtpV+cXC6QQHe8JaYTJZ2WLh07Yfrcy/wKi6SXR+DPE0g==;EndpointSuffix=core.windows.net"
    
    start, finish = (2008, 2021)
    # for yr in [1970, 2001, 2008, 2021]:
    for yr in range(start, finish + 1):
        if 2008 <= yr <= 2021:
            fname = f'f923_{yr}.zip'
        elif 2001 <= yr < 2008:
            fname = f'f906920_{yr}.zip'
        elif 1970 <= yr < 2001:
            fname = f'f759{yr}u.xls'
        else:
            raise Exception("Year {yr} is out of range")

        if f'{yr}' in ('2020', '2021'):
            src = f'https://www.eia.gov/electricity/data/eia923/xls/{fname}'
        elif 2001 <= yr < 2020:
            src = f'https://www.eia.gov/electricity/data/eia923/archive/xls/{fname}'
        elif 1970 <= yr < 2001:
            src = f'https://www.eia.gov/electricity/data/eia923/archive/xls/utility/{fname}'
        else:
            raise Exception("Year {yr} is out of range")

        csvname = fname.split('.')[0] + '.csv'
        ufp = f'/home/azureuser/electricity/data/{fname}'
        err = dict()
        try:
            retrieve_file(src, ufp)
            unzip_file(ufp)
            write_csv(yr, csvname)
            time.sleep(random.randrange(1, 6))
            upload_file(container_name='electricity-data',
                        local_file_name=csvname, 
                        upload_file_path=f'/home/azureuser/electricity/data/{csvname}', 
                        overwrite=True)
            os.remove(ufp)
            os.remove(f'/home/azureuser/electricity/data/{csvname}')
            shutil.rmtree(f'/home/azureuser/electricity/data/tmp')
        except Exception as ex:
            print(ex)
            err[f'{yr}'] = ex
            continue

    # # Nonutility prior to 2001:
    # for yr in [1989, 1999, 2000, 'notes']:
    #     if yr in ['notes']:
    #         fname = 'eia906dbnotes.xls'
    #         src = f'https://www.eia.gov/electricity/data/eia923/xls/{fname}'
    #     else:
    #         fname = f'f906nonutil{yr}.zip'
    #         src = f'https://www.eia.gov/electricity/data/eia923/archive/xls/{fname}'
        
    #     ufp = f'/home/azureuser/electricity/data/{fname}'
    #     err = dict()
    #     try:
    #         retrieve_file(src, ufp)
    #         time.sleep(random.randrange(1, 6))
    #         upload_file(container_name='electricity-data',
    #                     local_file_name=fname, upload_file_path=ufp, overwrite=True)
    #         os.remove(ufp)
    #     except Exception as ex:
    #         print(ex)
    #         err[f'nonutility_{yr}'] = ex
    #         continue

    # # Create the BlobServiceClient object which will be used to create a container client
    # # connect_str = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client('electricity-data')
    # List the blobs in the container
    blob_list = container_client.list_blobs()
    print("\n")
    for blob in blob_list:
        print("\t" + blob.name)

    print(f'\nErrors:\n\t{err}')


