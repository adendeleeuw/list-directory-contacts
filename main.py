import json, requests, time, sys, linecache
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

#--------Main Function to Grab Distribution IDs----------
def listContacts(qtoken, baseUrl, poolId):

    baseUrl = baseUrl
    headers = {
    "x-api-token": qtoken,
    "Content-Type": 'application/json'
     }
    nextPage = "init"

    while nextPage:
        try:
            response = requests.get(baseUrl, headers=headers)
            responseJson = response.json()
            responseArray = responseJson['result']['elements']
            nextPage = responseJson['result']['nextPage']
            baseUrl = nextPage  
        except:
            pass

        with ThreadPoolExecutor(max_workers=len(responseArray1)) as executor:
            for i in range(0, len(responseArray1)):
                executor.submit(getContact, headers, responseArray1, i, poolId)
               
#-------Function to grab individual distribution history---------
def getContact(headers, responseArray1, i, poolId):
    id = responseArray1[i]['contactId']
    baseUrl = f"https://au1.qualtrics.com/API/v3/directories/{poolId}/contacts/{id}"
    try: 
        response = requests.get(baseUrl, headers=headers)
        responseJson = response.json()
        responseArray2 = responseJson['result']['elements']
        responseArrayObj = pd.DataFrame(responseArray2)
        responseArrayObj.to_csv('distributions.csv', mode='a', encoding='utf-8')
    except:
        pass


def main():
    start_time = time.time()
    qtoken = "" #replace with your own Qualtrics token
    poolId = "SV_2ugOyY1cg44HKQZ"
    baseUrl = f"https://ca1.qualtrics.com/API/v3/directories/{poolId}/contacts"
    listContacts(qtoken, baseUrl, poolId)
    print("---Execution time: %s seconds ---" % (time.time() - start_time))

if __name__== "__main__":
    main()
