#standard libraries
import pandas as pd, numpy as np

#logging
from loguru import logger

#working with the web
import requests
import json

#confidential
from dotenv import dotenv_values

#user-made
from user_fx_for_creating_statements import remove_all_blanks
from user_fx_for_creating_statements import check_for_spaces_in_the_counterparty
from user_fx_for_creating_statements import check_for_spaces_in_the_organisation
from user_fx_for_creating_statements import convert_date_to_right_format

from user_fx_for_creating_statements import obtain_records

from user_fx_for_creating_statements import export_n_fill_currency

from user_fx_for_creating_statements import find_elements_from_completion
from user_fx_for_creating_statements import extract_calculation_object_key_from_completion

from user_fx_for_creating_statements import find_hidden_contract_amount_in_receipts
from user_fx_for_creating_statements import extract_elements_from_receipts_with_all_contracts

from user_fx_for_creating_statements import find_elements_from_offsetting_of_debts
from user_fx_for_creating_statements import find_hidden_amount_in_offsetting

from user_fx_for_creating_statements import find_elements_from_implementation_adjustment
from user_fx_for_creating_statements import create_connected_documents_feature

from user_fx_for_creating_statements import extract_report_number
from user_fx_for_creating_statements import create_document_amount
from user_fx_for_creating_statements import create_debt_amount

from download_statements import download_document

#working with dates
from datetime import datetime

#store results
from collections import OrderedDict





#set the credentials
credentials = dotenv_values()


#create the logging report
logger.add('log/reconciliation_report',
           format="{time} {level} {message}")


#create an ordered dict to store reports
error_statistics = OrderedDict([
    ('Контрагент', []),
    ('Код Договора', []),
    ('Дата Договора', []),
    ('Организация', [])
    ]
)


original_folder_path = credentials['original_folder_path']

#change the file name every coming month
original_file_name_title = credentials['original_file_name_title']
original_file_name = original_file_name_title + '.xlsx'

#combine all sheet data into one table
combined_table = pd.concat(pd.read_excel(original_folder_path + original_file_name,
                                         sheet_name=None), ignore_index=True)
logger.success('combined all sheet data into one table')

combined_table = remove_all_blanks(combined_table)

combined_table['Контрагент'] = combined_table.apply(check_for_spaces_in_the_counterparty,
                                                    axis=1)

combined_table['Организация'] = combined_table.apply(check_for_spaces_in_the_organisation,
                                                     axis=1)

combined_table['Дата Договора'] = combined_table['Дата Договора'].astype('string')

#force the change > remove timedelta
combined_table['Дата Договора'] = combined_table['Дата Договора'].apply(lambda x: str(x)[:10])
combined_table['Дата Договора'] = combined_table.apply(convert_date_to_right_format,
                                                       axis=1)

combined_table['Дата Договора'] = combined_table['Дата Договора'].astype('string')
combined_table['Номер Договора'] = combined_table['Номер Договора'].astype('string')


dupl_columns = list(combined_table.columns)
combined_table = combined_table.drop_duplicates(subset=dupl_columns)
logger.success(f'duplicates removed (if any)')

full_path = credentials['full_path']
combined_file_name = f'combined_output.xlsx'

combined_table.to_excel(full_path + combined_file_name,
                        index=False,
                        header=True)

logger.success(f'exported the combined table: {combined_file_name}')


authorization_key = credentials['authorization_key']
base_system = credentials['base_system'] #база 1С
report_calculation_type = credentials['calculation_type'] #тип расчетов

headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ru-RU,ru;q=0.9',
        'Authorization': 'Basic ' + authorization_key + '=',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'kraglin',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
}


currency_key = export_n_fill_currency(base_system,
                                      headers)

if currency_key != 0:
    logger.success(f'obtained the currency key: {currency_key}')
else:
    logger.error('did not obtain the currency key')


through_the_looking_glass = obtain_records(full_path,
                                           combined_file_name)



#start the process of creation of statements
for n in range(len(through_the_looking_glass)):
    try:
          
        reconciliation = {
            'Date': None,
            'Организация_Key': None,
            'Контрагент': None,
            'Контрагент_Type': 'StandardODATA.Catalog_Контрагенты',
            'ТипРасчетов': '',
            'Партнер_Key': '00000000-0000-0000-0000-000000000000',
            'Договор': '',
            'Договор_Type': 'StandardODATA.Undefined',
            'НачалоПериода': None,
            'КонецПериода': None,
            'Валюта_Key': None,
            'Комментарий': '',
            'Руководитель_Key': None,
            'ГлавныйБухгалтер_Key': None,
            'ОтветственноеЛицо': 'Руководитель',
            'РазбиватьПоТипамРасчетов': 'false',
            'РазбиватьПоПартнерам': 'false',
            'РазбиватьПоДоговорам': 'false',
            'Автор_Key': '46fc4472-c10b-11ee-8201-005056a29841',
            'ИтоговыеЗаписи': [{}],
            'ДетальныеЗаписи': {}
            }



        o_url = 'http://kraglin/' + base_system + f"/odata/standard.odata/Catalog_Организации?$format=json&$filter=Description eq '{through_the_looking_glass[n]['organisation']}'&$select=Ref_Key"
        logger.info(f'{n} > organisation url: {o_url}')
        response_o = requests.get(o_url,
                                headers=headers)

        if response_o.status_code == 200 or response_o.status_code == 201:
            logger.success(f'{n} > organisation response: {response_o.status_code}')
            organisation_details = json.loads(response_o.content.decode('utf-8'))
            
            if len(organisation_details['value']) != 0:
                organisation_key = organisation_details['value'][0]['Ref_Key']
            else:
                organisation_key = 0

        else:
            logger.error(f'{n} > no organisation response: {response_o.status_code}')

        logger.info(f'{n} > organisation name: {through_the_looking_glass[n]['organisation']}')
        logger.info(f'{n} > organisation_key: {organisation_key}')

        if organisation_key != 0:
            logger.success(f'{n} > <attained the organisation key>')
        else:
            logger.error(f'{n} > <did not attain the organisation key>')



        if organisation_key != 0:
            head_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Catalog_ОтветственныеЛицаОрганизаций?$format=json&$filter=Owner_Key eq guid'{organisation_key}' and ОтветственноеЛицо eq 'Руководитель' and ДатаОкончания eq datetime'0001-01-01T00:00:00'&$select=Ref_Key, Description"
            logger.info(f'{n} > company head url: {head_url}')
            
            head_persons_response = requests.get(head_url,
                                                 headers=headers)
                    
            if head_persons_response.status_code == 200 or head_persons_response.status_code == 201:
                logger.success(f'{n} > company head response with organisation key: {head_persons_response.status_code}')
                company_head_details = json.loads(head_persons_response.content.decode('utf-8'))
                
                if len(company_head_details['value']) != 0:
                    head_key = company_head_details['value'][0]['Ref_Key']
                    head_description = company_head_details['value'][0]['Description']

                    logger.info(f'{n} > company head key: {head_key}')
                    logger.info(f'{n} > company head: {head_description}')

                else:
                    head_key = 0
                    head_description = 0

                    logger.info(f'{n} > no company head records: {company_head_details}')

            else:
                logger.error(f'{n} > no company head response with organisation key: {head_persons_response.status_code}')
    
        else:
            logger.error(f'{n} > company head and description did not generate, thus reconciliation report cannot be build without person responsible')



        if organisation_key != 0:
            accountant_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Catalog_ОтветственныеЛицаОрганизаций?$format=json&$filter=Owner_Key eq guid'{organisation_key}' and ОтветственноеЛицо eq 'ГлавныйБухгалтер' and ДатаОкончания eq datetime'0001-01-01T00:00:00'&$select=Ref_Key, Description"
            logger.info(f'{n} > head accountant url: {accountant_url}')
            
            accountant_persons_response = requests.get(accountant_url,
                                                       headers=headers)
                    
            if accountant_persons_response.status_code == 200 or accountant_persons_response.status_code == 201:
                logger.success(f'{n} > head accountant response with organisation key: {accountant_persons_response.status_code}')
                accountant_details = json.loads(accountant_persons_response.content.decode('utf-8'))
                
                if len(accountant_details['value']) != 0:
                    accountant_key = accountant_details['value'][0]['Ref_Key']
                    accountant_description = accountant_details['value'][0]['Description']
                    logger.info(f'{n} > head accountant key: {accountant_key}')
                    logger.info(f'{n} > head accountant: {accountant_description}')

                else:
                    accountant_key = 0
                    accountant_description = 0

                    logger.info(f'{n} > no head accountant records: {accountant_details}')

            else:
                logger.error(f'{n} > no head accountant response with organisation key: {accountant_persons_response.status_code}')
    
        else:
            logger.error(f'{n} > head accountant and description did not generate, thus reconciliation report cannot be build without person responsible')



        cp_url = 'http://kraglin/' + base_system + f"/odata/standard.odata/Catalog_Контрагенты?$format=json&$filter=Description eq '{through_the_looking_glass[n]['counterparty']}'&$select=Ref_Key, Партнер_Key"

        logger.info(f'{n} > counterparty url: {cp_url}')
        response_cp = requests.get(cp_url,
                                headers=headers)

        if response_cp.status_code == 200 or response_cp.status_code == 201:
            logger.success(f'{n} > counterparty response: {response_cp.status_code}')
            counterparty_details = json.loads(response_cp.content.decode('utf-8'))
            
            if len(counterparty_details['value']) != 0:
                counterparty_ref_key = counterparty_details['value'][0]['Ref_Key']
                partner_ref_key = counterparty_details['value'][0]['Партнер_Key']
            else:
                counterparty_ref_key = 0
                partner_ref_key = 0
        
        else:
            logger.error(f'{n} > no counterparty response: {response_cp.status_code}')

        logger.info(f'{n} > counterparty name: {through_the_looking_glass[n]['counterparty']}')
        logger.info(f'{n} > counterparty_key: {counterparty_ref_key}')
        logger.info(f'{n} > partner_key: {partner_ref_key}')

        if counterparty_ref_key != 0:
            logger.success(f'{n} > <attained the counterparty and partner key>')
        else:
            logger.error(f'{n} > <did not attain the counterparty and partner key>')

    


        url = "http://kraglin/" + base_system + f"/odata/standard.odata/Catalog_ДоговорыКонтрагентов?$format=json&$filter=Номер eq '{through_the_looking_glass[n]['contract_code']}' and Контрагент_Key eq guid'{counterparty_ref_key}' and Дата eq datetime'{through_the_looking_glass[n]['contract_date']}'&$select=Ref_Key, Дата, НаименованиеДляПечати, ОбъектРасчетов_Key, ЗакупкаПодДеятельностьОпределяетсяВДокументе"
        logger.info(f'{n} > contract code url: {url}')
        response = requests.get(url,
                                headers=headers)

        if response.status_code == 200 or response.status_code == 201:
            logger.success(f'{n} > contract code response: {response.status_code}')
            contract_details = json.loads(response.content.decode('utf-8'))

            if response.json()["value"] != []:
                contract_ref_key = contract_details['value'][0]['Ref_Key']
                contract_date = contract_details['value'][0]['Дата']
                contract_print = contract_details['value'][0]['НаименованиеДляПечати']
                calculation_object_key_for_receipts = contract_details['value'][0]['ОбъектРасчетов_Key'] #changed from taking the object_key from receipt report (as it's not always the same)
                search_by_contract_code = through_the_looking_glass[n]['contract_code']
                purchasing_under_activity = contract_details['value'][0]['ЗакупкаПодДеятельностьОпределяетсяВДокументе']

                logger.success(f'{n} > <attained the contract code key>')
            else:
                contract_ref_key = 0
                contract_date = 0
                contract_print = 0
                calculation_object_key_for_receipts = 0
                purchasing_under_activity = 0

                logger.error(f'{n} > <did not attain the contract code key>')
                    
            logger.info(f'{n} > contract code: {through_the_looking_glass[n]['contract_code']}')
            logger.info(f'{n} > contract code key: {contract_ref_key}')
            logger.info(f'{n} > contract name for print: {contract_print}')
            logger.info(f'{n} > contract date: {through_the_looking_glass[n]['contract_date']}')
            logger.info(f'{n} > contract date is a match? - {through_the_looking_glass[n]['contract_date'] == contract_date}')
            logger.info(f'{n} > invoice made? - {purchasing_under_activity}')
    



        if counterparty_ref_key != 0 and partner_ref_key != 0:
            complete_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Document_АктВыполненныхРабот?$format=json&$filter=Контрагент_Key eq guid'{counterparty_ref_key}' and Партнер_Key eq guid'{partner_ref_key}' and Договор_Key eq guid'{contract_ref_key}' and DeletionMark ne true&$select=Ref_Key, Number, Date, Валюта_Key, СуммаВзаиморасчетов, СуммаДокумента, Договор_Key, Услуги"
            logger.info(f'{n} > completion report url: {complete_url}')
            complete_response = requests.get(complete_url,
                                             headers=headers)

            if complete_response.status_code == 200 or complete_response.status_code == 201:
                logger.success(f'{n} > completion response with contract code: {complete_response.status_code}')
                completion_details = json.loads(complete_response.content.decode('utf-8'))
                
                if len(completion_details['value']) != 0:
                    complete_doc_ref_key = find_elements_from_completion(completion_details, 'Ref_Key')
                    doc_number = find_elements_from_completion(completion_details, 'Number')
                    dates = find_elements_from_completion(completion_details, 'Date')
                    complete_currency = find_elements_from_completion(completion_details, 'Валюта_Key')
                    settlement_amount = find_elements_from_completion(completion_details, 'СуммаВзаиморасчетов')
                    doc_of_completion_amount = find_elements_from_completion(completion_details, 'СуммаДокумента')
                else:
                    complete_doc_ref_key = 0
                    doc_number = 0
                    dates = 0
                    complete_currency = 0
                    settlement_amount = 0
                    doc_of_completion_amount = 0
            
            else:
                logger.error(f'{n} > no completion response with contract code: {complete_response.status_code}')
            
            if len(complete_doc_ref_key) != 0 or len(doc_number) != 0 or len(dates) != 0 or len(settlement_amount) != 0 or len(doc_of_completion_amount) != 0:
                logger.success(f'{n} > <attained the completion report with contract code>')
            else:
                logger.error(f'{n} > <did not attain the completion report with contract code>')
        else:
            logger.error(f'counterparty and partney key with contract code did not generate, thus completion report cannot be build')
    



        if counterparty_ref_key != 0 and partner_ref_key != 0:
            complete_services_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Document_АктВыполненныхРабот?$format=json&$filter=Контрагент_Key eq guid'{counterparty_ref_key}' and Партнер_Key eq guid'{partner_ref_key}' and Договор_Key eq guid'{contract_ref_key}' and DeletionMark ne true&$select=Услуги"
            complete_services_response = requests.get(complete_services_url,
                                                      headers=headers)

            if complete_services_response.status_code == 200 or complete_services_response.status_code == 201:
                logger.success(f'{n} > completion services response with contract code: {complete_services_response.status_code}')
                completion_services_details = json.loads(complete_services_response.content.decode('utf-8'))
                
                if len(completion_services_details['value']) != 0:
                    calculation_object_key = extract_calculation_object_key_from_completion(completion_services_details)
                    logger.info(f'{n} > calculation object key: {calculation_object_key}')
                else:
                    calculation_object_key = 0
            
            else:
                logger.error(f'{n} > no completion services response with contract code: {complete_services_response.status_code}, thus no calculation object key')
        else:
            logger.error(f'{n} > no calculation object key was attained: {complete_services_response.status_code}')




        if counterparty_ref_key != 0 and partner_ref_key != 0:
            invoice_for_payment_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Document_СчетНаОплатуКлиенту?$format=json&$filter=Контрагент_Key eq guid'{counterparty_ref_key}' and Договор_Key eq guid'{contract_ref_key}' and DeletionMark ne true&$select=Ref_Key"
            logger.info(f'{n} > invoice for payment url: {invoice_for_payment_url}')
            invoice_for_payment_response = requests.get(invoice_for_payment_url,
                                                        headers=headers)

            list_of_invoice_keys = list()

            if invoice_for_payment_response.status_code == 200 or invoice_for_payment_response.status_code == 201:
                logger.success(f'{n} > invoice for payment response with counterparty and contract code: {invoice_for_payment_response.status_code}')
                invoice_for_payment_details = json.loads(invoice_for_payment_response.content.decode('utf-8'))
                
                if len(invoice_for_payment_details['value']) != 0:
                    for v in invoice_for_payment_details['value']:
                        list_of_invoice_keys.append(v['Ref_Key'])
                    logger.info(f'{n} > list of unique invoice payment keys were generated')
                else:
                    list_of_invoice_keys = 0
            else:
                logger.error(f'{n} > no invoice for payment response with counterparty and contract code: {invoice_for_payment_response.status_code}, thus no invoice payments')
        else:
            logger.error(f'{n} > no invoice payments were attained: {invoice_for_payment_response.status_code}')




        if counterparty_ref_key != 0 and partner_ref_key != 0:
            receipt_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Document_ПоступлениеБезналичныхДенежныхСредств?$format=json&$filter=Контрагент_Key eq guid'{counterparty_ref_key}' and DeletionMark ne true&$select=Ref_Key, Number, Date, Организация_Key, БанковскийСчет_Key, СуммаДокумента, Контрагент_Key, Валюта_Key, НомерВходящегоДокумента, ДатаВходящегоДокумента, ДатаПроведенияБанком, РасшифровкаПлатежа"
            logger.info(f'{n} > receipt report url: {receipt_url}')
            receipt_response = requests.get(receipt_url,
                                            headers=headers)
            
            if receipt_response.status_code == 200 or receipt_response.status_code == 201:
                logger.success(f'{n} > receipt response with contract code: {receipt_response.status_code}')
                receipt_details = json.loads(receipt_response.content.decode('utf-8'))
                
                if len(receipt_details['value']) != 0:
                    receipt_doc_ref_key = extract_elements_from_receipts_with_all_contracts(receipt_details, 'Ref_Key', counterparty_ref_key, contract_ref_key, list_of_invoice_keys)
                    bank_date = extract_elements_from_receipts_with_all_contracts(receipt_details, 'ДатаПроведенияБанком', counterparty_ref_key, contract_ref_key, list_of_invoice_keys)
                    contract_number = extract_elements_from_receipts_with_all_contracts(receipt_details, 'Number', counterparty_ref_key, contract_ref_key, list_of_invoice_keys)
                    receipt_full_amount = extract_elements_from_receipts_with_all_contracts(receipt_details, 'СуммаДокумента', counterparty_ref_key, contract_ref_key, list_of_invoice_keys)
                        
                    receipt_amount = find_hidden_contract_amount_in_receipts(receipt_details, 'Сумма', contract_ref_key, calculation_object_key_for_receipts, list_of_invoice_keys)
                    incoming_doc_number = extract_elements_from_receipts_with_all_contracts(receipt_details, 'НомерВходящегоДокумента', counterparty_ref_key, contract_ref_key, list_of_invoice_keys)
                    receipt_currency = extract_elements_from_receipts_with_all_contracts(receipt_details, 'Валюта_Key', counterparty_ref_key, contract_ref_key, list_of_invoice_keys)

                else:
                    receipt_doc_ref_key = 0
                    bank_date = 0
                    contract_number = 0
                    receipt_full_amount = 0

                    receipt_amount = 0
                    incoming_doc_number = 0
                    receipt_currency = 0

                    logger.info(f'{n} > no receipt records: {receipt_details}')
                            
            else:
                logger.error(f'{n} > no receipt response with contract code: {receipt_response.status_code}')
            

            if type(receipt_doc_ref_key) is int or type(bank_date) is int or type(contract_number) is int or type(receipt_amount) is int or type(incoming_doc_number) is int or type(receipt_currency) is int or type(receipt_full_amount) is int:
                logger.error(f'{n} > <did not attain the receipt report as there are no payments>')

            elif len(receipt_doc_ref_key) != 0 or len(bank_date) != 0 or len(contract_number) != 0 or len(receipt_amount) != 0 or len(incoming_doc_number) != 0 or len(receipt_currency) != 0 or len(receipt_full_amount) != 0:         
                logger.success(f'{n} > <attained the receipt report with contract code>')
            
            else:
                logger.error(f'{n} > <did not attain the receipt report with contract code>')

        else:
            logger.error(f'{n} > counterparty and partney key with contract code did not generate, thus receipt report cannot be build')

    


        if counterparty_ref_key != 0 and partner_ref_key != 0:
            offsetting_debt_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Document_ВзаимозачетЗадолженности?$format=json&$filter=КонтрагентКредитор eq cast(guid'{counterparty_ref_key}', 'Catalog_Контрагенты') and DeletionMark ne true&$select=Ref_Key, Number, Date, КонтрагентДебитор, КонтрагентКредитор, ДебиторскаяЗадолженность, КредиторскаяЗадолженность"
            logger.info(f'{n} > offsetting of debts > creditor report url: {offsetting_debt_url}')
            offsetting_debt_response = requests.get(offsetting_debt_url,
                                                    headers=headers)
        
            if offsetting_debt_response.status_code == 200 or offsetting_debt_response.status_code == 201:
                logger.success(f'{n} > offsetting response with CREDITOR counterparty and its catalogue: {offsetting_debt_response.status_code}')
                offsetting_by_debt_details = json.loads(offsetting_debt_response.content.decode('utf-8'))
                
                if len(offsetting_by_debt_details['value']) != 0: 
                    debt_ref_key = find_elements_from_offsetting_of_debts(offsetting_by_debt_details, 'Ref_Key')
                    creditor_counterparty = 'counterparty_is_a_creditor'
                    debt_number_document = find_elements_from_offsetting_of_debts(offsetting_by_debt_details, 'Number')
                    debt_date = find_elements_from_offsetting_of_debts(offsetting_by_debt_details, 'Date')
                    debt_of_mutual_settlements = find_hidden_amount_in_offsetting(offsetting_by_debt_details, 'СуммаВзаиморасчетов', creditor_counterparty, partner_ref_key, calculation_object_key)

                else:
                    debt_ref_key = 0
                    creditor_counterparty = 0
                    debt_number_document = 0
                    debt_date = 0
                    debt_of_mutual_settlements = 0

                    logger.info(f'{n} > no offsetting creditor records: {offsetting_by_debt_details}')
                            
            else:
                logger.error(f'{n} > no offsetting response with CREDITOR counterparty and its catalogue: {offsetting_debt_response.status_code}')
            

            if type(debt_ref_key) is int or type(creditor_counterparty) is int or type(debt_number_document) is int or type(debt_date) is int or type(debt_of_mutual_settlements) is int:
                logger.error(f'{n} > <did not attain the offsetting creditor report as there are no payments>')

            elif len(debt_ref_key) != 0 or len(creditor_counterparty) != 0 or len(debt_number_document) != 0 or len(debt_date) != 0 or len(debt_of_mutual_settlements) != 0:
                logger.success(f'{n} > <attained the offsetting creditor report as there are some payments>')
            
            else:
                logger.error(f'{n} > <did not attain the offsetting creditor report>')

        else:
            logger.error(f'{n} > counterparty and partney key did not generate, thus offsetting creditor report cannot be build')
        



        if counterparty_ref_key != 0 and partner_ref_key != 0:
            offsetting_advance_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Document_ВзаимозачетЗадолженности?$format=json&$filter=КонтрагентДебитор eq cast(guid'{counterparty_ref_key}', 'Catalog_Контрагенты') and DeletionMark ne true&$select=Ref_Key, Number, Date, КонтрагентДебитор, КонтрагентКредитор, ДебиторскаяЗадолженность, КредиторскаяЗадолженность"
            logger.info(f'{n} > offsetting of debts > debitor report url: {offsetting_advance_url}')
            offsetting_advance_response = requests.get(offsetting_advance_url,
                                                    headers=headers)
            
            if offsetting_advance_response.status_code == 200 or offsetting_advance_response.status_code == 201:
                logger.success(f'{n} > offsetting response with DEBITOR counterparty and its catalogue: {offsetting_advance_response.status_code}')
                offsetting_by_advance_details = json.loads(offsetting_advance_response.content.decode('utf-8'))
                
                if len(offsetting_by_advance_details['value']) != 0:
                    advance_ref_key = find_elements_from_offsetting_of_debts(offsetting_by_advance_details, 'Ref_Key')
                    debitor_counterparty = 'counterparty_is_a_debitor'
                    advance_number_document = find_elements_from_offsetting_of_debts(offsetting_by_advance_details, 'Number')
                    advance_date = find_elements_from_offsetting_of_debts(offsetting_by_advance_details, 'Date')
                    advance_of_mutual_settlements = find_hidden_amount_in_offsetting(offsetting_by_advance_details, 'СуммаВзаиморасчетов', debitor_counterparty, partner_ref_key, calculation_object_key_for_receipts)
                else:
                    advance_ref_key = 0
                    debitor_counterparty = 0
                    advance_number_document = 0
                    advance_date = 0
                    advance_of_mutual_settlements = 0

                    logger.info(f'{n} > no offsetting debitor records: {offsetting_by_advance_details}')      
            else:
                logger.error(f'{n} > no offsetting response with DEBITOR counterparty and its catalogue: {offsetting_advance_response.status_code}')
            
            if type(advance_ref_key) is int or type(debitor_counterparty) is int or type(advance_number_document) is int or type(advance_date) is int or type(advance_of_mutual_settlements) is int:
                logger.error(f'{n} > <did not attain the offsetting debitor report as there are no payments>')

            elif len(advance_ref_key) != 0 or len(debitor_counterparty) != 0 or len(advance_number_document) != 0 or len(advance_date) != 0 or len(advance_of_mutual_settlements) != 0:
                logger.success(f'{n} > <attained the offsetting debitor report as there are some payments>')
            
            else:
                    logger.error(f'{n} > <did not attain the offsetting debitor report>')

        else:
            logger.error(f'{n} > counterparty and partney key did not generate, thus offsetting debitor report cannot be build')

    


        if counterparty_ref_key != 0 and partner_ref_key != 0:
            implementation_adjustment_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Document_КорректировкаРеализации?$format=json&$filter=Контрагент_Key eq guid'{counterparty_ref_key}' and Партнер_Key eq guid'{partner_ref_key}' and Договор_Key eq guid'{contract_ref_key}' and DeletionMark ne true&$select=Ref_Key, Number, ДатаПлатежа, СуммаДокумента, СуммаВзаиморасчетов"
            logger.info(f'{n} > implementation adjustment url: {implementation_adjustment_url}')
            implementation_adjustment_response = requests.get(implementation_adjustment_url,
                                                            headers=headers)
            
            if implementation_adjustment_response.status_code == 200 or implementation_adjustment_response.status_code == 201:
                logger.success(f'{n} > implementation adjustment response: {implementation_adjustment_response.status_code}')
                    
                implementation_adjustment_details = json.loads(implementation_adjustment_response.content.decode('utf-8'))
                
                if len(implementation_adjustment_details['value']) != 0:
                    implement_ref_key = find_elements_from_implementation_adjustment(implementation_adjustment_details, 'Ref_Key')
                    implement_number_document = find_elements_from_implementation_adjustment(implementation_adjustment_details, 'Number')
                    implement_payment_date = find_elements_from_implementation_adjustment(implementation_adjustment_details, 'ДатаПлатежа')
                    implement_document_amount = find_elements_from_implementation_adjustment(implementation_adjustment_details, 'СуммаДокумента')
                    implement_settlement_amount = find_elements_from_implementation_adjustment(implementation_adjustment_details, 'СуммаВзаиморасчетов')
                else:
                    implement_ref_key = 0
                    implement_number_document = 0
                    implement_payment_date = 0
                    implement_document_amount = 0
                    implement_settlement_amount = 0

                    logger.info(f'{n} > no implementation adjustment records: {implementation_adjustment_details}')
                            
            else:
                logger.error(f'{n} > no implementation adjustment response: {implementation_adjustment_response.status_code}')
            
            if type(implement_ref_key) is int or type(implement_number_document) is int or type(implement_payment_date) is int or type(implement_document_amount) is int or type(implement_settlement_amount) is int:
                logger.error(f'{n} > <did not attain the implementation adjustment report as there are no discrepancies>')

            elif len(implement_ref_key) != 0 or len(implement_number_document) != 0 or len(implement_payment_date) != 0 or len(implement_document_amount) != 0 or len(implement_settlement_amount) != 0:
                logger.success(f'{n} > <attained the implementation adjustment report as there are some discrepancies>')
            
            else:
                logger.error(f'{n} > <did not attain the implementation adjustment report>')

        else:
            logger.error(f'{n} > counterparty and partney key did not generate, thus implementation adjustment report cannot be build')

    


        if type(receipt_amount) is int:
            turnover_debit = sum(settlement_amount)
            turnover_credit = 0

            if turnover_debit > turnover_credit:
                    final_balance_debit = turnover_debit - turnover_credit
                    final_balance_credit = 0
            elif turnover_debit < turnover_credit:
                    final_balance_credit = turnover_credit - turnover_debit
                    final_balance_debit = 0
            else:
                    final_balance_debit, final_balance_credit = 0, 0
            
            logger.success(f'{n} > calculated summary amount entries for reconciliation, incl. only completion amounts')

        elif len(settlement_amount) != 0 and len(receipt_amount) != 0:
            turnover_debit = sum(settlement_amount)
            turnover_credit = sum(receipt_amount)

            if turnover_debit > turnover_credit:
                    final_balance_debit = turnover_debit - turnover_credit
                    final_balance_credit = 0
            elif turnover_debit < turnover_credit:
                    final_balance_credit = turnover_credit - turnover_debit
                    final_balance_debit = 0
            else:
                    final_balance_debit, final_balance_credit = 0, 0
            
            logger.success(f'{n} > calculated the summary amount entries for reconciliation, incl. both completion and receipt amounts')
                
        else:
            logger.error(f'{n} > did not calculate the summary amount entries for reconciliation, check for prior connection errors')




        if type(debt_of_mutual_settlements) is int:
            turnover_debit = turnover_debit + 0
        elif debt_of_mutual_settlements != 0:
            turnover_debit = turnover_debit + sum(debt_of_mutual_settlements)

        if type(advance_of_mutual_settlements) is int:
            turnover_credit = turnover_credit + 0
        elif advance_of_mutual_settlements != 0:
            turnover_credit = turnover_credit + sum(advance_of_mutual_settlements)


        if turnover_debit > turnover_credit:
            final_balance_debit = turnover_debit - turnover_credit
            final_balance_credit = 0
        elif turnover_debit < turnover_credit:
            final_balance_credit = turnover_credit - turnover_debit
            final_balance_debit = 0
        else:
            final_balance_debit, final_balance_credit = 0, 0




        if type(implement_settlement_amount) is int:
            turnover_debit = turnover_debit + 0
        elif implement_settlement_amount != 0:
            for i_amount in implement_settlement_amount:
                if i_amount < 0: #if it's negative: counterparty pays less for the work done
                    turnover_credit = turnover_credit + abs(i_amount)
                    logger.info(f'{n} > if implementation adjustment is negative, counterparty pays less for the work done')
                    logger.info(f'{n} > {turnover_credit}')
                elif i_amount > 0: ##if it's positive: adjustments were made on the works done, thus counterparty pays more
                    turnover_debit = turnover_debit + i_amount
                    logger.info(f'{n} > if implementation adjustment is positive, adjustments were made on the work done, thus counterparty pays more')
                    logger.info(f'{n} > {turnover_debit}')


        if turnover_debit > turnover_credit:
            final_balance_debit = turnover_debit - turnover_credit
            final_balance_credit = 0
        elif turnover_debit < turnover_credit:
            final_balance_credit = turnover_credit - turnover_debit
            final_balance_debit = 0
        else:
            final_balance_debit, final_balance_credit = 0, 0


    

        summary_records = pd.DataFrame({
            'LineNumber': 1,
            'ТипРасчетов': report_calculation_type,
            'Партнер_Key': partner_ref_key,
            'Договор': contract_ref_key,
            'Договор_Type': 'StandardODATA.Catalog_ДоговорыКонтрагентов',
            'ОбъектРасчетов_Key': calculation_object_key,
            'Валюта_Key': '00000000-0000-0000-0000-000000000000',
            'НачальноеСальдоДт': 0,
            'НачальноеСальдоДтКонтрагент': 0,
            'НачальноеСальдоКт': 0,
            'НачальноеСальдоКтКонтрагент': 0,
            'ОборотДт': turnover_debit,
            'ОборотДтКонтрагент': 0,
            'ОборотКт': turnover_credit,
            'ОборотКтКонтрагент': 0,
            'КонечноеСальдоДт': final_balance_debit,
            'КонечноеСальдоДтКонтрагент': 0,
            'КонечноеСальдоКт': final_balance_credit,
            'КонечноеСальдоКтКонтрагент': 0,
            'НомерДоговора': search_by_contract_code,
            'НомерДоговораКонтрагент': '',
            'ДатаДоговора': contract_date,
            'ДатаДоговораКонтрагент': '0001-01-01T00:00:00',
            'НаименованиеДоговора': contract_print,
            'НаименованиеДоговораКонтрагент': ''
            },
            index=[0])
    


        reconciliation['ИтоговыеЗаписи'][0]['LineNumber'] = str(summary_records['LineNumber'][0])
        reconciliation['ИтоговыеЗаписи'][0]['ТипРасчетов'] = summary_records['ТипРасчетов'][0]
        reconciliation['ИтоговыеЗаписи'][0]['Партнер_Key'] = summary_records['Партнер_Key'][0]
        reconciliation['ИтоговыеЗаписи'][0]['Договор'] = summary_records['Договор'][0]
        reconciliation['ИтоговыеЗаписи'][0]['Договор_Type'] = summary_records['Договор_Type'][0]
        reconciliation['ИтоговыеЗаписи'][0]['ОбъектРасчетов_Key'] = summary_records['ОбъектРасчетов_Key'][0]
        reconciliation['ИтоговыеЗаписи'][0]['Валюта_Key'] = summary_records['Валюта_Key'][0]
        reconciliation['ИтоговыеЗаписи'][0]['НачальноеСальдоДт'] = float(str(round(summary_records['НачальноеСальдоДт'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['НачальноеСальдоДтКонтрагент'] = float(str(round(summary_records['НачальноеСальдоДтКонтрагент'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['НачальноеСальдоКт'] = float(str(round(summary_records['НачальноеСальдоКт'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['НачальноеСальдоКтКонтрагент'] = float(str(round(summary_records['НачальноеСальдоКтКонтрагент'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['ОборотДт'] = float(str(round(summary_records['ОборотДт'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['ОборотДтКонтрагент'] = float(str(round(summary_records['ОборотДтКонтрагент'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['ОборотКт'] = float(str(round(summary_records['ОборотКт'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['ОборотКтКонтрагент'] = float(str(round(summary_records['ОборотКтКонтрагент'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['КонечноеСальдоДт'] = float(str(round(summary_records['КонечноеСальдоДт'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['КонечноеСальдоДтКонтрагент'] = float(str(round(summary_records['КонечноеСальдоДтКонтрагент'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['КонечноеСальдоКт'] = float(str(round(summary_records['КонечноеСальдоКт'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['КонечноеСальдоКтКонтрагент'] = float(str(round(summary_records['КонечноеСальдоКтКонтрагент'][0], 2)))
        reconciliation['ИтоговыеЗаписи'][0]['НомерДоговора'] = summary_records['НомерДоговора'][0]
        reconciliation['ИтоговыеЗаписи'][0]['НомерДоговораКонтрагент'] = summary_records['НомерДоговораКонтрагент'][0]
        reconciliation['ИтоговыеЗаписи'][0]['ДатаДоговора'] = summary_records['ДатаДоговора'][0]
        reconciliation['ИтоговыеЗаписи'][0]['ДатаДоговораКонтрагент'] = summary_records['ДатаДоговораКонтрагент'][0]
        reconciliation['ИтоговыеЗаписи'][0]['НаименованиеДоговора'] = summary_records['НаименованиеДоговора'][0]
        reconciliation['ИтоговыеЗаписи'][0]['НаименованиеДоговораКонтрагент'] = summary_records['НаименованиеДоговораКонтрагент'][0]

        if len(reconciliation['ИтоговыеЗаписи']) != 0:
            logger.success(f'{n} > reconciliation: summary entries have been created')
        else:
            logger.error(f'{n} > reconciliation: summary entries have not been created')




        if len(completion_details['value']) != 0:
            
            complete_records_lst = []

            clms = ['ДатаДокумента', 'СуммаДебет', 'СуммаКредит', 'НомерДокумента', 'РасчетныйДокумент', 'РасчетныйДокумент_Type', 'НаименованиеДокумента']
            cmpl_title_name = 'StandardODATA.Document_АктВыполненныхРабот'
            cmlt_doc_title = 'Акт выполненных работ'

            for days, settle_amount, doc_no, doc_ref_key in zip(dates, settlement_amount, extract_report_number(organisation_key, doc_number), complete_doc_ref_key):
                complete_records_lst.append([days, settle_amount, 0, doc_no, doc_ref_key, cmpl_title_name, cmlt_doc_title])

            completion_records = pd.DataFrame(complete_records_lst,
                                            columns=clms)


            receipt_records_lst = []

            clms = ['ДатаДокумента', 'СуммаДебет', 'СуммаКредит', 'НомерДокумента', 'РасчетныйДокумент', 'РасчетныйДокумент_Type', 'НаименованиеДокумента']
            rcpt_title_name = 'StandardODATA.Document_ПоступлениеБезналичныхДенежныхСредств'
            rcpt_doc_title = 'Платежное поручение'


            if type(receipt_amount) is int:
                receipt_records = pd.DataFrame()

            else:
                for days, receipt_amt, r_doc_number, r_doc_ref_key in zip(bank_date, receipt_amount, incoming_doc_number, receipt_doc_ref_key):
                    receipt_records_lst.append([days, 0, receipt_amt, r_doc_number, r_doc_ref_key, rcpt_title_name, rcpt_doc_title])

                    receipt_records = pd.DataFrame(receipt_records_lst,
                                                columns=clms)
            

            if len(completion_records) != 0 and len(receipt_records) != 0:
                final_acts = pd.concat([completion_records, receipt_records],
                                        axis=0)
                
                dupl_columns = list(final_acts.columns)
                final_acts = final_acts.drop_duplicates(subset=dupl_columns)

            else:
                final_acts = pd.concat([completion_records])

                dupl_columns = list(final_acts.columns)
                final_acts = final_acts.drop_duplicates(subset=dupl_columns)
            
            logger.success(f'{n} > completion and receipt reports were created')

        else:
            logger.error(f'{n} > completion and receipt reports were not created')




        if (type(debt_of_mutual_settlements) is int):
            offsetting_debt_records = pd.DataFrame()
        else:
            offsetting_debt_lst = []

            clms = ['ДатаДокумента', 'СуммаДебет', 'СуммаКредит', 'НомерДокумента', 'РасчетныйДокумент', 'РасчетныйДокумент_Type', 'НаименованиеДокумента']
            offs_title_name = 'StandardODATA.Document_ВзаимозачетЗадолженности'
            offs_doc_title = 'Взаимозачет Задолженности'

            for days, debt_amount, doc_debt_no, doc_debt_ref_key in zip(debt_date, debt_of_mutual_settlements, extract_report_number(organisation_key, debt_number_document), debt_ref_key):
                offsetting_debt_lst.append([days, debt_amount, 0, doc_debt_no, doc_debt_ref_key, offs_title_name, offs_doc_title])

            offsetting_debt_records = pd.DataFrame(offsetting_debt_lst,
                                                columns=clms)

        if (type(advance_of_mutual_settlements) is int):
            offsetting_advance_records = pd.DataFrame()
        else:
            offsetting_advance_lst = []

            clms = ['ДатаДокумента', 'СуммаДебет', 'СуммаКредит', 'НомерДокумента', 'РасчетныйДокумент', 'РасчетныйДокумент_Type', 'НаименованиеДокумента']
            offs_title_name = 'StandardODATA.Document_ВзаимозачетЗадолженности'
            offs_doc_title = 'Взаимозачет Задолженности'

            for days_a, advance_amount, doc_advance_no, doc_advance_ref_key in zip(advance_date, advance_of_mutual_settlements, extract_report_number(organisation_key, advance_number_document), advance_ref_key):
                offsetting_advance_lst.append([days_a, 0, advance_amount, doc_advance_no, doc_advance_ref_key, offs_title_name, offs_doc_title])

            offsetting_advance_records = pd.DataFrame(offsetting_advance_lst,
                                                    columns=clms)
            


        if len(offsetting_debt_records) != 0 and len(offsetting_advance_records) != 0:
            offsetting_of_debts = pd.concat([offsetting_debt_records, offsetting_advance_records],
                                            axis=0)
            
            dupl_columns = list(offsetting_of_debts.columns)
            offsetting_of_debts = offsetting_of_debts.drop_duplicates(subset=dupl_columns)

        elif len(offsetting_debt_records) != 0 or len(offsetting_advance_records) != 0:
            if len(offsetting_debt_records) != 0:
                offsetting_of_debts = pd.concat([offsetting_debt_records])

                dupl_columns = list(offsetting_of_debts.columns)
                offsetting_of_debts = offsetting_of_debts.drop_duplicates(subset=dupl_columns)
            elif len(offsetting_advance_records) != 0:
                offsetting_of_debts = pd.concat([offsetting_advance_records])

                dupl_columns = list(offsetting_of_debts.columns)
                offsetting_of_debts = offsetting_of_debts.drop_duplicates(subset=dupl_columns)
        else:
            offsetting_of_debts = pd.DataFrame()

        if offsetting_of_debts.empty:
            logger.error(f'{n} > no offsetting of debts report was generated, as no records were found')
        else:
            logger.success(f'{n} > offsetting of debts report was created')




        if (type(implement_settlement_amount) is int):
            implementation_adjustment = pd.DataFrame()
            logger.error(f'{n} > no implementation adjustment report was generated, as no records were found')

        else:
            implementation_adjustment_lst = []

            clms = ['ДатаДокумента', 'СуммаДебет', 'СуммаКредит', 'НомерДокумента', 'РасчетныйДокумент', 'РасчетныйДокумент_Type', 'НаименованиеДокумента']
            impl_adj_title_name = 'StandardODATA.Document_КорректировкаРеализации'
            impl_doc_title = 'Корректировка задолженности'

            for days, i_amount, doc_impl_no, doc_impl_ref_key in zip(implement_payment_date, implement_settlement_amount, extract_report_number(organisation_key, implement_number_document), implement_ref_key):
                if i_amount < 0: #if implementation_adjustment is negative: counterparty pays less for the work done
                    implementation_adjustment_lst.append([days, 0, abs(i_amount), doc_impl_no, doc_impl_ref_key, impl_adj_title_name, impl_doc_title])
                else:
                    implementation_adjustment_lst.append([days, i_amount, 0, doc_impl_no, doc_impl_ref_key, impl_adj_title_name, impl_doc_title])

            implementation_adjustment = pd.DataFrame(implementation_adjustment_lst,
                                                    columns=clms)
            
            logger.success(f'{n} > implementation adjustment report was created')



        if offsetting_of_debts.empty and implementation_adjustment.empty:
            detailed_entries = final_acts
            logger.info(f'{n} > neither offsetting of debts nor implementation adjustment were created, thus no additional records were added to reconciliation')

        else:
            final_acts = pd.concat([final_acts, offsetting_of_debts, implementation_adjustment],
                                    axis=0)

            dupl_columns = list(final_acts.columns)
            final_acts = final_acts.drop_duplicates(subset=dupl_columns)

            detailed_entries = final_acts
            logger.success(f'{n} > offsetting of debts and implementation adjustment were created, thus additional records were added to reconciliation')



        detailed_entries['СуммаДокумента'] = detailed_entries.apply(create_document_amount,
                                                                    axis=1)

        detailed_entries['СуммаДолг'] = detailed_entries.apply(create_debt_amount,
                                                            axis=1)
    


        if purchasing_under_activity == True:
            detailed_entries['СвязанныеДокументы'] = detailed_entries.apply(create_connected_documents_feature,
                                                                            axis=1)
        elif purchasing_under_activity == False:
            detailed_entries['СвязанныеДокументы'] = None
        else:
            pass


    
        detailed_entries['ТипРасчетов'] = report_calculation_type
        detailed_entries['Партнер_Key'] = partner_ref_key
        detailed_entries['ОбъектРасчетов_Key'] = calculation_object_key ###ADDED HERE
        detailed_entries['Договор'] = contract_ref_key
        detailed_entries['Договор_Type'] = 'StandardODATA.Catalog_ДоговорыКонтрагентов'
        detailed_entries['ВалютаДокумента_Key'] = currency_key #hopefully it's all one currency
        detailed_entries['СуммаАванс'] = 0
        detailed_entries['НомерДокументаКонтрагент'] = ''
        detailed_entries['ДатаДокументаКонтрагент'] = '0001-01-01T00:00:00'
        detailed_entries['НаименованиеДокументаКонтрагент'] = ''
        detailed_entries['СуммаДебетКонтрагент'] = 0
        detailed_entries['СуммаКредитКонтрагент'] = 0

        detailed_entries = detailed_entries.sort_values(by='ДатаДокумента')
        detailed_entries = detailed_entries.reset_index()
        detailed_entries['id'] = detailed_entries.index + 1
        detailed_entries = detailed_entries.drop(['index'],
                                                axis=1,
                                                errors='ignore')


        detailed_entries = detailed_entries.iloc[:, [8, 9, 10, 11, 12, 4, 5, 13, 6, 7, 14, 3, 15, 0, 16, 17, 18, 1, 19, 2, 20, 21]]
        d_entry_records = detailed_entries.shape[0]

        document_amount = round(detailed_entries['СуммаДокумента'].sum(), 2)
        debt_amount = round(detailed_entries['СуммаДолг'].sum(), 2)


        detailed_entries['ДатаДокумента'] = detailed_entries['ДатаДокумента'].apply(lambda x: x[:10])
        detailed_entries['ДатаДокумента'] = detailed_entries['ДатаДокумента'].apply(lambda x: str(x) + 'T00:00:00')
        detailed_entries['ДатаОперации'] = detailed_entries['ДатаДокумента']


        for d in range(d_entry_records):
                if len(reconciliation['ДетальныеЗаписи']) == 0:
                        reconciliation['ДетальныеЗаписи'] = [{'LineNumber': d+1}]
                else:
                        reconciliation['ДетальныеЗаписи'] += [{'LineNumber': d+1}]


                if reconciliation['ДетальныеЗаписи'][d]['LineNumber'] == d+1:
                        for key, value in detailed_entries.items():
                                
                                if key == 'ТипРасчетов':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
                                
                                elif key == 'Партнер_Key':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
                                
                                elif key == 'ОбъектРасчетов_Key':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
                                
                                elif key == 'Договор':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
                                
                                elif key == 'Договор_Type':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
                                
                                elif key == 'РасчетныйДокумент':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]

                                elif key == 'РасчетныйДокумент_Type':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
                                
                                elif key == 'ВалютаДокумента_Key':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
                                
                                elif key == 'СуммаДокумента':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = float(str(detailed_entries[key][d]))
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += float(str(detailed_entries[key][d]))
                                
                                elif key == 'СуммаДолг':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = float(str(detailed_entries[key][d]))
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += float(str(detailed_entries[key][d]))
                                
                                elif key == 'СуммаАванс':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = float(str(detailed_entries[key][d]))
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += float(str(detailed_entries[key][d]))

                                elif key == 'НомерДокумента':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
                                
                                elif key == 'НомерДокументаКонтрагент':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]

                                elif key == 'ДатаДокумента':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]

                                elif key == 'ДатаОперации':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]

                                elif key == 'ДатаДокументаКонтрагент':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
                                
                                elif key == 'НаименованиеДокумента':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]

                                elif key == 'НаименованиеДокументаКонтрагент':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]

                                elif key == 'СуммаДебет':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = float(str(detailed_entries[key][d]))
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += float(str(detailed_entries[key][d]))

                                elif key == 'СуммаДебетКонтрагент':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = float(str(detailed_entries[key][d]))
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += float(str(detailed_entries[key][d]))

                                elif key == 'СуммаКредит':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = float(str(detailed_entries[key][d]))
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += float(str(detailed_entries[key][d]))
                                
                                elif key == 'СуммаКредитКонтрагент':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = float(str(detailed_entries[key][d]))
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += float(str(detailed_entries[key][d]))
                                
                                elif key == 'СвязанныеДокументы':
                                        if key not in reconciliation['ДетальныеЗаписи'][d]:
                                                reconciliation['ДетальныеЗаписи'][d][key] = detailed_entries[key][d]
                                        else:
                                                reconciliation['ДетальныеЗаписи'][d][key] += detailed_entries[key][d]
        else:
                pass
    

        current_date = str(datetime.now())
        posting_date = current_date[:10] + 'T' + current_date[11:13] + ':' + current_date[14:16] + ':' + current_date[17:19]

        reconciliation['Организация_Key'] = organisation_key
        reconciliation['Контрагент'] = counterparty_ref_key
        reconciliation['Валюта_Key'] = currency_key
        reconciliation['Date'] = posting_date
        reconciliation['НачалоПериода'] = contract_date
        reconciliation['КонецПериода'] = current_date[:10] + 'T' + '00:00:00'
        reconciliation['Руководитель_Key'] = head_key
        reconciliation['ГлавныйБухгалтер_Key'] = accountant_key


        if len(reconciliation['ДетальныеЗаписи']) != 0:
            logger.success(f'{n} > reconciliation: detailed entries have been created')
        else:
            logger.error(f'{n} > reconciliation: detailed entries have not been created')

        logger.success(f'{n} > reconciliation report has been created')

        json_data = reconciliation

        pre_post_url = "http://kraglin/" + base_system + "/odata/standard.odata/Document_СверкаВзаиморасчетов2_5_11?$format=json"

        pre_post_response = requests.post(pre_post_url,
                                          headers=headers,
                                          json=json_data)

        if pre_post_response.status_code == 200 or pre_post_response.status_code == 201:
            logger.success(f'{n} > success: pre_post_response content, all creation elements populated - {pre_post_response.status_code}')
        else:
            logger.error(f'{n} > fail: pre_post_response content not populated - {pre_post_response.status_code}')
            logger.info(pre_post_response.__dict__)

        json_data = json.loads(pre_post_response.content.decode('utf-8'))


        guid_certificate = json_data['Ref_Key']
        number_act = json_data['Number']

        logger.success(f'{n} > number_act generated: {number_act}')
        logger.success(f'{n} > guid_certificate generated: {guid_certificate}')



        post_url = "http://kraglin/" + base_system + f"/odata/standard.odata/Document_СверкаВзаиморасчетов2_5_11(guid'{guid_certificate}')/Post?PostingModeOperational=false"
        post_response = requests.post(post_url,
                                      headers=headers)

        client = through_the_looking_glass[n]["counterparty"]
        date = str(datetime.now().strftime('%d.%m.%Y'))

        logger.success(f'{n} > success: post_response > report sent to 1C - {post_response.status_code}')

        file_certificate = download_document(guid_certificate,
                                             client,
                                             number_act,
                                             date)
    
    except:
        error_statistics['Контрагент'].append(through_the_looking_glass[n]['counterparty'])
        error_statistics['Код Договора'].append(through_the_looking_glass[n]['contract_code'])
        error_statistics['Дата Договора'].append(through_the_looking_glass[n]['contract_date'])
        error_statistics['Организация'].append(through_the_looking_glass[n]['organisation'])

        logger.critical(f'{n} > reconciliation report not generated')
        logger.critical(f'{n} > did not find counterparty: {through_the_looking_glass[n]['counterparty']} or contract: {through_the_looking_glass[n]['contract_code']} or organisation: {through_the_looking_glass[n]['organisation']}')
        
        pass


error_statistics_summary = pd.DataFrame(error_statistics)

errors_file_name = f'comedy_of_errors.xlsx'

error_statistics_summary.to_excel(full_path + errors_file_name,
                                  index=False,
                                  header=True)