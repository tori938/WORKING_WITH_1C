#email related
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

#working with tables
import pandas as pd

#working with system
from os import listdir
import pathlib

#confidential
from dotenv import dotenv_values

#logging
from loguru import logger





#set up the logger file
logger.add('log/sending_report',
           format="{time} {level} {message}")

#set the credentials
credentials = dotenv_values()



def send_email(receiver_address: str,
               directory_to_statements: str,
               reconciliation_file_name: str):
    '''
        > set up the connection to server and create part of the message with an attachment

    Parameters:
        receiver_address (str): e-mail address of the receiver (counterparty)
        directory_to_statements (str): full directory of folder + file name
        reconciliation_file_name (str): title of the reconciliation statement

    Output:
        message (str + pdf): output message to receiver
    '''

    #set up the account details
    mailer_address = credentials['email']
    mailer_password = credentials['password']
    smtp_server = credentials['server']
    port = 25
    receiver_copy_address = credentials['copy_email']

    #set up the communicate channel
    server = smtplib.SMTP(smtp_server,
                          port)
    
    #introduce the login data
    server.login(mailer_address,
                 mailer_password)
    
    #initialize the message object
    msg = MIMEMultipart()
    msg['From'] = mailer_address
    msg['To'] = receiver_address
    msg['Subject'] = f'Сверка Взаиморасчетов за весь период'
    msg['Cc'] = receiver_copy_address

    #initialize the base class
    part = MIMEBase('application', "octet-stream")
    with open(directory_to_statements, "rb") as f:
        part.set_payload(f.read())
    encoders.encode_base64(part)

    part.add_header("Content-Disposition", "attachment",
                    filename=reconciliation_file_name)
    msg.attach(part)

    message_folder = credentials['message_directory']
    message_file_name = 'email_message.htm'

    with open(message_folder + '\\' + message_file_name,
              'r') as text_message:
        message = text_message.read()
    
    #if the whole message is in text format, or if a part of it is
    msg.attach(MIMEText(message, 'html'))
    server.send_message(msg)
    server.quit()



def obtain_client_email(table: pd.DataFrame):

    '''
        > convert table data set into a dict taking counterparty and its email address
    '''

    #set up a dict
    client_emails = {}

    #take counterparty with its email address
    for row in range(len(table['Контрагент'])):
        client_emails[table['Контрагент'][row]] = {'email' : table['Адрес электронной почты'][row]}

    return client_emails



def send_document():

    '''
        > send reconciliation statement to the relevant counterparty
    '''
    
    statements_folder = str(pathlib.Path(credentials['path_acts']))
    full_path = credentials['full_path']

    combined_file_name = 'combined_output.xlsx'

    check_file = pd.read_excel(full_path + combined_file_name)

    #convert table to dict > take necessary elements
    client_post_data = obtain_client_email(check_file)

    #list of statements in the directory
    files = listdir(statements_folder)
    
    #if directory isn't empty
    while files != []:

        #take the 1st file (in order)
        file = files[0]
        
        if 'Сверка' in file:
            #extract counterparty name from the file_name
            counterparty = file[:file.index("Сверка")].replace("_", " ")[:-1]

            #if client exists in the list of clients and has a relative email address
            if counterparty in client_post_data and '@' in client_post_data[counterparty]['email']:
                logger.info(f'send reconciliation statement - {file} > to this email address: {client_post_data[counterparty]['email']}')
                
                send_email(receiver_address=client_post_data[counterparty]['email'],
                           directory_to_statements=statements_folder + '\\' + file,
                           reconciliation_file_name=file)
                logger.success(f'reconciliation statement - {file} sent to {client_post_data[counterparty]['email']}')
            else:
                logger.error(f'reconciliation statement - {file} not sent to {client_post_data[counterparty]['email']}')
        else:
            logger.error(f'{file} not found or incorrect file format')
        
    
        #remove the file from the memory and restart the process
        files.remove(file)

send_document()