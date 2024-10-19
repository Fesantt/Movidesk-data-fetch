import os
import requests
import pandas as pd
import time
from tqdm import tqdm
from pandas import json_normalize
from datetime import datetime
import pytz
import re
import json
from colorama import Fore, Style

def print_header():
    header = """       
                ███████ ███████    ███████  █████  ███    ██ ████████ ████████                     
                ██      ██         ██      ██   ██ ████   ██    ██       ██                        
█████ █████     █████   █████      ███████ ███████ ██ ██  ██    ██       ██        █████ █████     
                ██      ██              ██ ██   ██ ██  ██ ██    ██       ██                        
                ██      ███████ ██ ███████ ██   ██ ██   ████    ██       ██  
    """
    print(Fore.GREEN + header + Style.RESET_ALL)
    print("Moviget CW -- coded by FeSantt. GITHUB.COM/FESANTT")
    print("---------------------------------------------------")
    print("https://github.com/Fesantt/Movidesk-data-fetch")
    print("---------------------------------------------------")
    print(" ")

print_header()

def get_user_input():
    while True:
        access_token = input("Digite o Access Token da API Movidesk: ")
        if not access_token:
            print(Fore.RED + "O Access Token não pode ser vazio. Por favor, digite um valor válido." + Style.RESET_ALL)
            continue
        else:
            break

    while True:
        start_date = input("Digite a data de início (formato YYYY-MM-DD): ")
        end_date = input("Digite a data de término (formato YYYY-MM-DD): ")
        date_pattern = r"^\d{4}-\d{2}-\d{2}$"
        if not re.match(date_pattern, start_date) or not re.match(date_pattern, end_date):
            print(Fore.RED + "Formato de data inválido. Use o formato YYYY-MM-DD." + Style.RESET_ALL)
            continue
        else:
            break

    while True:
        try:
            ranges = int(input("Digite a quantidade de requisições (cada requisição retorna no máximo 1000 itens): "))
            if ranges <= 0:
                print(Fore.RED + "Por favor, digite um número positivo para a quantidade de requisições." + Style.RESET_ALL)
                continue
            else:
                break
        except ValueError:
            print(Fore.RED + "Por favor, digite um número inteiro para a quantidade de requisições." + Style.RESET_ALL)

    while True:
        increment_option = input("Deseja incrementar os dados em uma tabela existente? (sim/não): ").strip().lower()
        if increment_option not in ["sim", "não"]:
            print(Fore.RED + "Por favor, digite 'sim' ou 'não' para a opção de incremento." + Style.RESET_ALL)
            continue
        else:
            break

    if increment_option == "sim":
        while True:
            existing_file_name = input("Digite o nome do arquivo Excel existente (com extensão .xlsx): ")
            if not existing_file_name.endswith(".xlsx"):
                print(Fore.RED + "O arquivo deve ter a extensão .xlsx." + Style.RESET_ALL)
                continue
            else:
                new_file_name = existing_file_name
                break
    else:
        while True:
            new_file_name = input("Digite o nome do novo arquivo Excel (sem espaços e sem caracteres especiais): ")
            if not new_file_name.endswith(".xlsx"):
                new_file_name += ".xlsx"
            existing_file_name = None
            break
        
    return start_date, end_date, ranges, new_file_name, access_token, increment_option, existing_file_name

# Chamada da função e armazenamento dos resultados
start_date, end_date, ranges, new_file_name, access_token, increment_option, existing_file_name = get_user_input()


# Definindo filtros e Paramentros da Consulta na API
start_date = f"{start_date}T00:00:00.00z"
end_date = f"{end_date}T23:59:59.00z"
date_filter = f"createdDate ge {start_date} and createdDate le {end_date}"

base_url = "https://{seu_subdominio}.movidesk.com/public/v1/tickets/past"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

select_params = ("id,protocol,type,subject,serviceFull,category,urgency,status,subject,baseStatus,justification,lifetimeWorkingTime,stoppedTime,stoppedTimeWorkingTime,"
                 "origin,createdDate,createdBy,tags,resolvedIn,reopenedIn,closedIn,lastUpdate,"
                 "chatTalkTime,chatWaitingTime&$expand=owner,clients($select=businessName,phone),customFieldValues,actions($select=description)")


# Fazendo requisições na API - Parte facil :D
def fetch_data(skip):
    url = (f"{base_url}?token={access_token}&$select={select_params}&$filter={date_filter}&$top=1000&$skip={skip}")
    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            # Caso uma requisição de erro a mesma é repetida em 5s, evitando retrabalho
            print(f"Erro ao buscar dados, tentando novamente Aguarde .....")
            time.sleep(5) 


# Verificando horario, pois API Movdesk tem sistema de To many requests durante o dia :(
def is_night_time():
    brasilia_tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(brasilia_tz)
    return now.hour >= 19 or now.hour < 7

all_data = []

sleep_time = 12 if not is_night_time() else 0

total_time = ranges * (sleep_time if sleep_time > 0 else 1)

# ♾
with tqdm(total=total_time, desc='Progresso Total', unit='s') as total_pbar:
    for i in range(ranges):
        skip = i * 1000
        data = fetch_data(skip)
        if data:
            all_data.extend(data)
        
        if sleep_time > 0:
            with tqdm(total=sleep_time, desc=f'Tarefa: {i+1}/{ranges}', leave=False, unit='s') as sleep_pbar:
                for _ in range(sleep_time):
                    time.sleep(1)
                    sleep_pbar.update(1)
                    total_pbar.update(1)
        else:
            total_pbar.update(1)
            

# Tratando os dados - deu trabalho viu :( kkkkkk
df = json_normalize(all_data, sep='_')

def extract_client_info(clients, key):
    if clients:
        return ', '.join([str(client[key]) for client in clients if key in client and client[key]])
    return ''

if 'clients' in df.columns:
    df['Nomes dos Clientes'] = df['clients'].apply(lambda clients: extract_client_info(clients, 'businessName'))
    df['Telefones dos Clientes'] = df['clients'].apply(lambda clients: extract_client_info(clients, 'phone'))

if 'serviceFull' in df.columns:
    df['serviceFull'] = df['serviceFull'].apply(lambda services: ', '.join(services) if services else '')

if 'tags' in df.columns:
    df['tags'] = df['tags'].apply(lambda tags: ', '.join(tags) if tags else '')

if 'origin' in df.columns:
    df['origin'] = df['origin'].apply(lambda x: 'Whatsapp' if x == 23 else 'Chat Online' if x == 5 else x)

def convert_to_minutes(seconds):
    return float(seconds) / 60 if seconds else None

if 'chatWaitingTime' in df.columns:
    df['chatWaitingTime'] = df['chatWaitingTime'].apply(convert_to_minutes)

if 'chatTalkTime' in df.columns:
    df['chatTalkTime'] = df['chatTalkTime'].apply(convert_to_minutes)

def extract_custom_field_value(custom_field_values):
    if custom_field_values:
        for field in custom_field_values:
            if field['customFieldId'] == 112640:
                return str(field['value'])
    return ''

if 'customFieldValues' in df.columns:
    df['customFieldValues'] = df['customFieldValues'].apply(extract_custom_field_value)

# Função maldita pra converter Actions>description em um array de objetos e coletar alguns dados 
def extract_message_times(actions):
    welcome_time = None
    last_message_time = None
    if isinstance(actions, list): 
        for action in actions:
            description = action.get('description', '')
            
            # Aqui obtenho a data/hora da primeira mensagem do atendente
            welcome_pattern = r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2}) - [^:]+: .*?Seja bem-vindo\(a\) ao Suporte da Cardápio Web.*'
            welcome_matches = re.findall(welcome_pattern, description, re.DOTALL)
            if welcome_matches:
                welcome_time = welcome_matches[0] 
            
            # Aqui obtenho a data/hora da ultima mensagem 
            message_pattern = r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2}) - [^:]+: .*?(?=(\r\n\d{2}/\d{2}/\d{4} \d{2}:\d{2} - |\r\n$))'
            messages = re.findall(message_pattern, description, re.DOTALL)
            if messages:
                last_message_time = messages[-1][0]  

    return welcome_time, last_message_time

df[['welcome_message_time', 'last_message_time']] = df['actions'].apply(lambda x: pd.Series(extract_message_times(x)))
df['welcome_message_time'] = pd.to_datetime(df['welcome_message_time'], format='%d/%m/%Y %H:%M')
df['last_message_time'] = pd.to_datetime(df['last_message_time'], format='%d/%m/%Y %H:%M')

df.drop('actions', axis=1, inplace=True)

# Facilitando a vida do Usuario
column_mapping = {
    "id": "Ticket",
    "protocol": "Protocolo",
    "type": "Tipo",
    "subject": "Assunto",
    "serviceFull": "Serviço Completo",
    "category": "Categoria",
    "urgency": "Urgência",
    "status": "Status",
    "baseStatus": "Status Base",
    "justification": "Justificativa",
    "origin": "Origem",
    "createdDate": "Data de Criação",
    "createdBy": "Criado Por",
    "tags": "Tags",
    "resolvedIn": "Resolvido Em",
    "reopenedIn": "Reaberto Em",
    "closedIn": "Fechado Em",
    "lastUpdate": "Última Atualização",
    "chatTalkTime": "Tempo de Conversa do Chat",
    "chatWaitingTime": "Tempo de Espera do Chat",
    "customFieldValues": "Nota Wpp",
    "owner_id": "ID do Atendente",
    "owner_businessName": "Atendente",
    "owner_email": "Email do Atendente",
    "owner_phone": "Telefone do Atendente",
    "welcome_message_time": "Horario Primeira Resposta",
    "last_message_time": "Horario Ultima Resposta",   
}

df.rename(columns=column_mapping, inplace=True)

# convertendo data em UTC que é retornada da API para GMT-3
if 'Data de Criação' in df.columns:
    brasilia_tz = pytz.timezone('America/Sao_Paulo')
    df['Data de Criação'] = pd.to_datetime(df['Data de Criação']).dt.tz_localize('UTC').dt.tz_convert(brasilia_tz).dt.tz_localize(None)

if 'Horario Primeira Resposta' in df.columns and 'Data de Criação' in df.columns:

    df['Primeira Resp (segundos)'] = (df['Horario Primeira Resposta'] - df['Data de Criação']).dt.total_seconds()

    df['Primeira Resp (minutos)'] = df['Primeira Resp (segundos)'] / 60

    df['Primeira Resp (segundos)'] = df['Primeira Resp (segundos)'].apply(lambda x: max(x, 0))
    df['Primeira Resp (minutos)'] = df['Primeira Resp (minutos)'].apply(lambda x: max(x, 0))

if increment_option == 'sim':
    if existing_file_name and os.path.exists(existing_file_name):
        existing_df = pd.read_excel(existing_file_name)
        df = pd.concat([existing_df, df])
        df.drop_duplicates(subset=["Ticket"], inplace=True)
    else:
        print("Arquivo existente não encontrado. Criando um novo arquivo.")
        
df.to_excel(new_file_name, index=False)

# Se chegou aqui deu tudo Certo :)
print(Fore.GREEN + f"Tarefa concluída, dados salvos no arquivo {new_file_name}" + Style.RESET_ALL)

pause = input("Pressione ENTER para sair...")
