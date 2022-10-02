import os
import pickle
# Gmail API utils
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
# for encoding/decoding messages in base64
from base64 import urlsafe_b64decode
import logging

# Request all access (permission to read/send/receive emails, manage the inbox, and more)
SCOPES = ['https://mail.google.com/']
our_email = 'lee.t.truong@gmail.com'

def setup_logger():
    logger = logging.getLogger("gmail_read")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("debug.log")
    sh = logging.StreamHandler()
    logger.addHandler(fh)
    logger.addHandler(sh)
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    return logger

def gmail_authenticate():
    creds = None
    # the file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    pickle_file = os.getenv('TKN_PICKLE', 'token.pickle')
    if os.path.exists(pickle_file):
        with open(pickle_file, "rb") as token:
            creds = pickle.load(token)
    # if there are no (valid) credentials availablle, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
               os.getenv('CRD_JSON', 'credentials.json'), SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open(pickle_file, "wb") as token:
            pickle.dump(creds, token)
    return build('gmail', 'v1', credentials=creds)


def search_messages(service, query):
    result = service.users().messages().list(userId='me', q=query).execute()
    messages = []
    if 'messages' in result:
        messages.extend(result['messages'])
    while 'nextPageToken' in result:
        page_token = result['nextPageToken']
        result = service.users().messages().list(
            userId='me', q=query, pageToken=page_token).execute()
        if 'messages' in result:
            messages.extend(result['messages'])
    return messages


def main():

    # authenticate with gmail
    service = gmail_authenticate()

    # search for messages that are unread
    smart_meter_emails = search_messages(
        service=service, query='subject:Smart Meter Texas – Subscription Report and is:unread')

    files = []

    for smart_meter_email in smart_meter_emails:
        message = service.users().messages().get(userId='me', id=smart_meter_email['id'], format='full').execute()

        for part in message['payload']['parts']:
            if part.get('filename') and 'csv' in part.get('filename').lower():
                files.append(
                    {
                        'filename' : part.get('filename'),
                        'messageId' : smart_meter_email.get('id'),
                        'attachmentId' : part['body'].get('attachmentId'), 
                    
                    }
                )
    for file in files:
        # grab attachment data
        attachment = service.users().messages().attachments().get(id=file.get('attachmentId'), userId='me', messageId=file.get('messageId')).execute()

        # output data to file
        if attachment.get('data'):
            with open(f'smartmeter/{file["filename"]}', 'w+') as f:
                data = urlsafe_b64decode(attachment.get('data'))
                f.write(data.decode())
        # mark email as read
        service.users().messages().modify(userId='me', id=file['messageId'], body={
            "removeLabelIds": ["UNREAD"]
        }).execute()

if __name__ == "__main__":
    logger = setup_logger()

    try:
        main()
    except Exception as error:
        logger.exception(f"{error}")
        exit(1)
