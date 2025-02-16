import pandas as pd

import requests
import json





def remove_all_blanks(table: pd.DataFrame):
    
    '''
        > remove rows where all values are missing (NaN) from each column

    Parameters:
        table (DataFrame): table with data from all sheets

    Output:
        table (DataFrame): table with no missing elements in rows or columns
    '''

    return table.dropna(how='all',
                        axis=0)



def check_for_spaces_in_the_counterparty(table: pd.DataFrame):
    
    '''
        > removes spaces in the counterparty name
    '''

    trim_feature = str(table['Контрагент']).lstrip().rstrip()
    
    return trim_feature



def check_for_spaces_in_the_organisation(table: pd.DataFrame):
    
    '''
        > removes spaces in the organisation name
    '''

    trim_feature = str(table['Организация']).lstrip().rstrip()
    
    return trim_feature



def convert_date_to_right_format(table: pd.DataFrame):

    '''
        > convert the contract date into the format in 1C

    Parameters:
        table (DataFrame): table with data from all sheets
    
    Output:
        date (str): YYYY-MM-DDT00:00:00
    '''

    return str(table['Дата Договора']) + 'T00:00:00'



def obtain_records(check_directory: str,
                   file_name: str):
    
    '''
        > extract only necessary elements needed for 1C queries from the table data set

    Parameters:
        check_directory (str): directory to file, i.e. table with data from all sheets
        file_name (str): name of the file
    
    Output:
        dict with necessary elements for 1C queries, i.e. counterparty, contract code, contract date, organisation
        > future: potentially to add calculation type
    '''

    path_to_file = check_directory + file_name
    
    table = pd.read_excel(path_to_file)

    d_data = {}

    for row in range(len(table['Контрагент'])):
        d_data[row] = {'counterparty': table['Контрагент'][row],
                       'contract_code': table['Номер Договора'][row],
                       'contract_date': table['Дата Договора'][row],
                       'organisation': table['Организация'][row]
                       }
            
    return d_data



def export_n_fill_currency(base_system: str,
                           headers: dict):
    
    '''
        > a query to return the currency reference key in 1C
    
    Parameters:
        base_system (str): 1C base system
        headers (dict): HTTP headers that contain (confidential) metadata that are sent along with HTTP requests and responses
    
    Output:
        key (str): currency reference key
    '''
    
    url = "http://kraglin/" + base_system + "/odata/standard.odata/Catalog_Валюты?$format=json&$select=Ref_Key"
    
    response = requests.get(url,
                            headers=headers)

    currency_details = json.loads(response.content.decode('utf-8'))
    
    return currency_details['value'][0]['Ref_Key']



def find_elements_from_completion(document,
                                  feature: str):

    '''
    > extract a list of elements per counterparty

    Parameters:
        document (json): report in question, i.e. certificate of completion of works
        feature (json key): the extracted value, i.e. ref_key, date (..)

    Output:
        list of elements (list)

    '''
    
    elem_lst = list()

    length = len(document['value'])
    iter = 0

    while iter < length:
        for i in range(len(document['value'])):
            for key in document['value'][i]:
                iter += 1
            elem_lst.append(document['value'][i][feature])
    
    return elem_lst



def extract_calculation_object_key_from_completion(document):

    '''
    > extract the calculation object key per counterparty

    Parameters:
        document (json): report in question, i.e. certificate of completion of works

    Output:
        calculation object key (string)

    '''
    
    elem_lst = list()

    length = len(document['value'])
    iter = 0

    while iter < length:
        for i in range(len(document['value'])):
            for key in document['value'][i]:
                iter += 1
            elem_lst.append(document['value'][i]['Услуги'][0]['ОбъектРасчетов_Key'])
    
    return elem_lst[0]



def extract_elements_from_receipts_with_all_contracts(document,
                                                      feature: str,
                                                      counterparty_key: str,
                                                      contract_key: str,
                                                      invoice_keys: list):
    
    '''
    > extract a list of elements per counterparty

    Parameters:
        document (json): report in question, i.e. receipt of non-cash funds
        feature (json key): the extracted value, i.e. ref_key, date (..)
        counterparty_key (str): counterparty unique reference key
        contract_key (str): contract code unique reference key
        invoice_keys (list): list of invoice keys, i.e. for those accounts that get paid / covered by a third party

    Output:
        list of elements (list)

    '''

    elem_lst = list()
    
    for i in document['value']:
        if i['Контрагент_Key'] == counterparty_key:
            if "ОснованиеПлатежа" in i["РасшифровкаПлатежа"][0]:
                for n in range(len(i['РасшифровкаПлатежа'])):
                    if i["РасшифровкаПлатежа"][n]["ОснованиеПлатежа_Type"] == 'StandardODATA.Catalog_ДоговорыКонтрагентов' and i["РасшифровкаПлатежа"][n]["ОснованиеПлатежа"] == contract_key:
                            elem_lst.append(i[feature])
                    #should not cause an error if doesn't exist, right? (hopefully???)
                    elif i["РасшифровкаПлатежа"][n]["ОснованиеПлатежа_Type"] == 'StandardODATA.Document_СчетНаОплатуКлиенту' and i['РасшифровкаПлатежа'][n]['ОснованиеПлатежа'] in invoice_keys:
                        elem_lst.append(i[feature])

    return elem_lst



def find_hidden_contract_amount_in_receipts(document,
                                            feature: str,
                                            contract_key: str,
                                            object_key: str,
                                            invoice_keys: list):

    '''
    > extract a list of elements per counterparty

    Parameters:
        document (json): report in question, i.e. receipt of non-cash funds
        feature (json key): the extracted value, i.e. (hidden) amount relevant by contract
        contract_key (str): contract code unique reference key
        object_key (str): calculation object unique reference key > taken directly / specifically from catalogue of contracts
        invoice_keys (list): list of invoice keys, i.e. for those accounts that get paid / covered by a third party

    Output:
        list of elements (list)

    '''

    elem_lst = list()

    for n in range(len(document['value'])):
        for key in document['value'][n]:
            if key == 'РасшифровкаПлатежа':
                for nn in range(len(document['value'][n][key])):
                    if document['value'][n][key][nn]["ОснованиеПлатежа_Type"] == 'StandardODATA.Catalog_ДоговорыКонтрагентов' and document['value'][n][key][nn]['ОснованиеПлатежа'] == contract_key and document['value'][n][key][nn]['ОбъектРасчетов_Key'] == object_key:
                        elem_lst.append(document['value'][n][key][nn][feature])
                    elif document['value'][n][key][nn]["ОснованиеПлатежа_Type"] == 'StandardODATA.Document_СчетНаОплатуКлиенту' and document['value'][n][key][nn]['ОснованиеПлатежа'] in invoice_keys and document['value'][n][key][nn]['ОбъектРасчетов_Key'] == object_key:
                        elem_lst.append(document['value'][n][key][nn][feature])
    
    return elem_lst



def find_elements_from_offsetting_of_debts(document,
                                           feature: str):

    '''
    > extract a list of elements per counterparty

    Parameters:
        document (json): report in question, i.e. offsetting of debts
        feature (json key): the extracted value, i.e. number, date

    Output:
        list of elements (list)

    '''
    
    elem_lst = list()

    length = len(document['value'])
    iter = 0

    while iter < length:
        for i in range(len(document['value'])):
            for key in document['value'][i]:
                iter += 1
            elem_lst.append(document['value'][i][feature])
    
    return elem_lst



def find_hidden_amount_in_offsetting(document,
                                     feature: str,
                                     counterparty_identificator: str,
                                     partner_key: str,
                                     object_key: str):

    '''
    > extract a list of elements per counterparty and partner

    Parameters:
        document (json): report in question, i.e. offsetting of debts
        feature (json key): the extracted value, i.e. (hidden) amount > debt or advancement (СуммаВзаиморасчетов)
        counterparty_identificator (str): is the counterparty a creditor or debitor?
        partner_key (str): partner reference key
        object_key (str): calculation object unique reference key > taken directly / specifically from completion report (or possibly from receipt report)

    Output:
        list of elements (list)

    '''

    elem_lst = list()

    for n in range(len(document['value'])):
        for key in document['value'][n]:
            #find the debt amount
            if counterparty_identificator == 'counterparty_is_a_creditor':
                if key == 'КредиторскаяЗадолженность':
                    for nn in range(len(document['value'][n][key])):
                        if document['value'][n][key][nn]['Партнер_Key'] == partner_key and document['value'][n][key][nn]['ОбъектРасчетов_Key'] == object_key:
                            elem_lst.append(document['value'][n][key][nn][feature])
            elif counterparty_identificator == 'counterparty_is_a_debitor':
                if key == 'ДебиторскаяЗадолженность':
                    for nn in range(len(document['value'][n][key])):
                        if document['value'][n][key][nn]['Партнер_Key'] == partner_key and document['value'][n][key][nn]['ОбъектРасчетов_Key'] == object_key:
                            elem_lst.append(document['value'][n][key][nn][feature])

    return elem_lst



def find_elements_from_implementation_adjustment(document,
                                                 feature: str):

    '''
    > extract a list of elements per counterparty

    Parameters:
        document (json): report in question, i.e. implementation adjustment
        feature (json key): the extracted value, i.e. number, date, settlement_amount

    Output:
        list of elements (list)

    '''
    
    elem_lst = list()

    length = len(document['value'])
    iter = 0

    while iter < length:
        for i in range(len(document['value'])):
            for key in document['value'][i]:
                iter += 1
            elem_lst.append(document['value'][i][feature])
    
    return elem_lst



def extract_report_number(organisation_key: str,
                          lst: list):

    '''
    > converts each document number to the relevant format

    Parameters:
        lst (list): list of document numbers
    
    Output:
        lst (list): list of document numbers without counteragent reference, i.e. 0T00
    
    '''
    if organisation_key == 'c3ed2bb7-bf45-11ee-8201-005056a29841': #Terra
        lst = [item.replace('0Т00-', '') for item in lst]
        found = [item.lstrip('0') for item in lst]
        return found
    elif organisation_key == '360e9c9a-bf84-11ee-8201-005056a29841': #Kvanta
        lst = [item.replace('0К00-', '') for item in lst]
        found = [item.lstrip('0') for item in lst]
        return found
    elif organisation_key == 'd2bd5e0c-c010-11ee-8201-005056a29841': #Rey
        lst = [item.replace('0Р00-', '') for item in lst]
        found = [item.lstrip('0') for item in lst]
        return found



def create_document_amount(table: pd.DataFrame):
    
    '''
        > compare and contrast debit and credit to create the document amount
    '''

    if table['НаименованиеДокумента'] != 'Корректировка задолженности':
        if table['СуммаКредит'] == 0:
            return table['СуммаДебет']
        elif table['СуммаКредит'] != 0:
            return table['СуммаКредит']
    else:
        return 0
        


def create_debt_amount(table: pd.DataFrame):
    
    '''
        > compare and contrast debit and credit to create the debt amount
    '''

    if table['НаименованиеДокумента'] != 'Корректировка задолженности':
        if table['СуммаКредит'] == 0:
            return table['СуммаДебет']
        elif table['СуммаКредит'] != 0:
            return - table['СуммаКредит']
    else:
        return 0
    


def create_connected_documents_feature(table: pd.DataFrame):
    
    '''
        > add an additional feature: connected documents to the reconciliation report based on the condition in the invoice identificator
    '''

    if table['НаименованиеДокумента'] == 'Акт выполненных работ' or table['НаименованиеДокумента'] == 'Корректировка задолженности':

        #extract the date
        short_d = table['ДатаДокумента'][8:10] + '.' + table['ДатаДокумента'][5:7] + '.' + table['ДатаДокумента'][:4]

        #create the connected document
        new_feature = 'Счет-фактура №' + str(table['НомерДокумента']) + ' от ' + short_d
        
        return new_feature