#!/home/azureuser/miniconda3/envs/electricity/bin/python

import os, requests, time, random
from azure.storage.blob import BlobServiceClient

def retrieve_file(dl_path, ul_path):
    try:
        resp = requests.get(dl_path, allow_redirects=False)
        with open(ul_path, 'wb') as obj:
            obj.write(resp.content)
        return None
    except Exception as ex:
        print(f"Exception: {ex}")


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
    
    start, finish = (1970, 2021)
    # for yr in range(start, finish + 1):
    for yr in [1970, 2001, 2008, 2021]:
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

        ufp = f'/home/azureuser/electricity/data/{fname}'
        err = dict()
        try:
            retrieve_file(src, ufp)
            time.sleep(random.randrange(1, 6))
            upload_file(container_name='electricity-data',
                        local_file_name=fname, upload_file_path=ufp, overwrite=True)
            os.remove(ufp)
        except Exception as ex:
            print(ex)
            err[yr] = ex
            continue

    # Create the BlobServiceClient object which will be used to create a container client
    # connect_str = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client('electricity-data')
    # List the blobs in the container
    blob_list = container_client.list_blobs()
    print("\n")
    for blob in blob_list:
        print("\t" + blob.name)

    print(f'\nErrors:\n\t{err}')


