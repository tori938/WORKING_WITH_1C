from requests import Session
from dotenv import dotenv_values

import os





credentials = dotenv_values()

base_system = credentials['base_system']



def get_decode_response(session,
                        url,
                        params,
                        headers,
                        data,
                        type):
    
    if type == "data":
        response = session.post(url,
                                params=params,
                                headers=headers,
                                data=data,
                                verify=False)
    else:
        response = session.post(url,
                                params=params,
                                headers=headers,
                                json=data,
                                verify=False)
    
    return response.content.decode("utf-8")



def get_values(response_decode,
               type):
    
    if type == "fid":
        fid = response_decode.split('"fid":')[1][1:37]
        remote_key = response_decode.split('"remotekey":')[1][1:37]
        fover = response_decode.split('"fover":')[1][1:37]
        
        return fid, remote_key, fover
    
    else:
        url = response_decode.split("e1cib")[1][:-2]
        
        return url



def replace_with_guid(data,
                      guid,
                      fid,
                      remote_key,
                      fover,
                      type_guid,
                      type_fid,
                      type_remote_key,
                      type_fover_key,
                      number,
                      date):

    data = data.replace(type_guid, guid)
    data = data.replace(type_fid, fid)
    data = data.replace(type_remote_key, remote_key)
    data = data.replace(type_fover_key, fover)
    data = data.replace("number", number)
    #data = data.replace("date", date)
    data = data.replace("date", str(date))
    
    return data



def get_content(session,
                url_certificate,
                headers):

    content_certificate = session.get(url_certificate,
                                      headers=headers).content
    
    return content_certificate



def save_documents(path_acts,
                   act):
    with open(path_acts, 'wb') as file:
        file.write(act)



def download_document(guid_certificate,
                      client,
                      number_act,
                      date):
    
    url = credentials["url"]
    
    session = Session()
    
    id_session = credentials["id_session"]
    
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json; charset=UTF-8',
        'Origin': 'http://kraglin',
        'Referer': 'http://kraglin/' + base_system + '/ru_RU/',
        'vrs-session': id_session
    }
    
    fid = ""
    remote_key = ""
    fover = ""
    for number in range(1, 4):
        data = replace_with_guid(str(credentials[f"data_{number}"]),
                                 guid_certificate,
                                 fid,
                                 remote_key,
                                 fover,
                                 "guid", "fid_replace", "remote_key", "fover_replace",
                                 number_act, date).encode()
        
        params = {
            'cmd': credentials[f"cmd_{number}"]
        }
        
        response_decode = get_decode_response(session,
                                              url,
                                              params,
                                              headers,
                                              data,
                                              "data")
        
        if fid == "":
            fid, remote_key, fover = get_values(response_decode, "fid")

    params = {
        'cmd': credentials[f"cmd_4"]
    }

    json_data = {
        'root': {
            'rKey': remote_key,
            'formuuid': fid,
            'props': {},
            'path': '10',
            'printType': 0,
            'isChrome': True,
        }
    }

    
    url = credentials["url_last"]
    response_decode = get_decode_response(session,
                                          url,
                                          params,
                                          headers,
                                          json_data,
                                          "json")
    
    url_certificate = get_values(response_decode,
                                 "url")

    path_acts = credentials['path_acts']

    client = client.replace('/', '')
    file_certificate = f"{path_acts}{client.replace(' ', '_')}_СверкаВзаиморасчетов_№{number_act}.pdf"
    url = credentials["url_download"]
    certificate = get_content(session, url + url_certificate,  headers)
    save_documents(file_certificate, certificate)

    return file_certificate