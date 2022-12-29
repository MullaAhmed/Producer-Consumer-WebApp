from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials


def create_folder(creds, folder_name):
    creds=creds

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {
            'name': 'Placeholder',
            'mimeType': 'application/vnd.google-apps.folder'
        }

        file_metadata['name'] = folder_name

        # pylint: disable=maybe-no-member
        file = service.files().create(body=file_metadata, fields='id'
                                      ).execute()
        print(f'Folder ID: "{file.get("id")}".')
        return file.get('id')

    except HttpError as error:
        print(f'An error occurred: {error}')
        return None

def share_file(real_file_id, real_user, real_domain,creds):
    
    creds=creds

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)
        ids = []
        file_id = real_file_id

        def callback(request_id, response, exception):
            if exception:
                print(exception)
            else:
                print(f'Request_Id: {request_id}')
                print(F'Permission Id: {response.get("id")}')
                ids.append(response.get('id'))

       
        batch = service.new_batch_http_request(callback=callback)
        user_permission = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': 'user@example.com'
        }
        
        user_permission['emailAddress'] = real_user
        
        batch.add(service.permissions().create(fileId=file_id,
                                               body=user_permission,
                                               fields='id',))
        
        if real_domain!=None:
            domain_permission = {
                'type': 'domain',
                'role': 'reader',
                'domain': 'example.com'
            }

            domain_permission['domain'] = real_domain
            batch.add(service.permissions().create(fileId=file_id,
                                                body=domain_permission,
                                                fields='id',))
            batch.execute()

    except HttpError as error:
        print(F'An error occurred: {error}')
        ids = None

    return ids

scope = ['https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive"]

cred=ServiceAccountCredentials.from_json_keyfile_name("fir-f9b2a-a37910c3c880.json", scope)
name="Atlan"

id=create_folder(cred,name)
share_file(real_file_id=id,real_user='ahmedmulla1910@gmail.com',real_domain=None,creds=cred)