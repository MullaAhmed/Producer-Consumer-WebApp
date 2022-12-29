from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
from kafka import KafkaConsumer
import json

# Creating dataframe from API Response
def create_dataframe(api_response):
    questions = {}
    index = []
    for response in list(api_response["_items"]):
        form_id = response['formId']
        for answer in (response['answers']):
            response_id = answer['responseId']
            question_id = answer['questionId']

            if "text" in answer:
                ans = (answer['text'])
            elif "media" in answer:
                ans = (answer['media'][0])
            elif "location" in answer:
                ans = str(answer['location']['coordinates'][0])

            if question_id in questions:
                questions[question_id].append(ans)
            elif question_id not in questions:
                questions[question_id] = [ans]

        index.append(response_id)

    data = pd.DataFrame()
    data["Users"] = index
    for x in questions:
        data[x] = pd.Series(questions[x])
    data = data.where(pd.notnull(data), None)
    
    return(data, form_id)


# Check if sheet exists
def sheet_check(name):
    drive = build('drive', 'v3', credentials=credentials)
    response = drive.files().list(
        q="parents='1GQghlQtdahhzfDC_uW-40FAVik_ehN6n'",).execute()
    files = response.get('files')
    file_found = 0
    for file in files:
        if file['name'] == name:
            file_found = 1
            id = (file['id'])
            break
    if file_found == 0:
        id,file_found=create_gsheet(name)
     
    return id,file_found


# Creating Google Sheet File
def create_gsheet(name):
    drive = build('drive', 'v3', credentials=credentials)
    file_metadata = {
        'name': name,
        'parents': ['1GQghlQtdahhzfDC_uW-40FAVik_ehN6n'],
        'mimeType': 'application/vnd.google-apps.spreadsheet',
    }
    res = drive.files().create(body=file_metadata).execute()
    return res['id'],0


def writing_gsheet(id, dataframe,file_found):
    gsheet_id = id
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    df = dataframe

    if file_found==0:                   #This will prevent from writing the heading again
        df_values=df.T.reset_index().T.values.tolist()
        print("1")
        print(df_values)
    else:
        df_values=df.values.tolist()
        print(df_values)

    result = sheet.values().append(
            spreadsheetId=gsheet_id,
            range="Sheet1!A1",
            valueInputOption="USER_ENTERED",
            body=dict(
                majorDimension='ROWS',
                values=df_values
            )
        ).execute()
    

hostname="localhost:9092" #Karafka server URL
topic_name="test1"        #Topic name

consumer = KafkaConsumer(topic_name,
auto_offset_reset='earliest',
 value_deserializer = lambda v: json.loads(v.decode('ascii')),
 key_deserializer = lambda v: json.loads(v.decode('ascii')),
 )
consumer.subscribe(topics=[topic_name])
consumer.subscription()


# Setting Up Credentials for Sheets
SCOPES = ['https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive']

credentials = service_account.Credentials.from_service_account_file(
    'fir-f9b2a-a37910c3c880.json', scopes=SCOPES)
    

for message in consumer:
    dataframe, form_id = create_dataframe(message.value)
    sheet_id,file_found = sheet_check(form_id)
    print(file_found)
    writing_gsheet(sheet_id, dataframe,file_found)