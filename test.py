import os, requests, zipfile

# for flist in ['f759' + str(yr) + 'u' for yr in range(1970, 2021)]:
#     fname = os.path.join('/home/azureuser/electricity/data', flist + '.xls')
#     if os.path.exists(fname):
#         os.remove(fname)
#     else:
#         print("The file does not exist")


def retrieve_file(dl_path, ul_path):
    try:
        resp = requests.get(dl_path, allow_redirects=False)
        with open(ul_path, 'wb') as obj:
            obj.write(resp.content)
        return None
    except Exception as ex:
        print(f"Exception: {ex}")

def unzip_file(file_name):
    local_path = '/home/azureuser/electricity/data'
    local_file_name = os.path.join(
        local_path, 
        file_name)
    with zipfile.ZipFile(local_file_name, 'r') as zfile:
        zfile.extractall(local_path)


fname = f'f923_2008.zip'
dlp = 'https://www.eia.gov/electricity/data/eia923/archive/xls/{fname}'
ufp = os.path.join('/home/azureuser/electricity/data', fname)

retrieve_file(dlp, ufp)
# unzip_file(ufp)
