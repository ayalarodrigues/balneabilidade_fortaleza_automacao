# scraper_historico.py (VERSÃO FINAL PARA GITHUB ACTIONS)

import gspread
import pandas as pd
import os
import io
import json
import httplib2
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from core_parser import processar_pdf_completo, extrair_metadados_pdf

# Aumenta o tempo de espera padrão para 60 segundos, sendo mais robusto
httplib2.Http.DEFAULT_TIMEOUT = 60

# --- Configurações ---
NOME_ARQUIVO_CREDENCIAL = "credentials.json"
NOME_PLANILHA = "balneabilidade_fortaleza" # <<<!!! TROQUE PELO NOME EXATO DA SUA PLANILHA !!!>>>
NOME_PAGINA = "DadosBalneabilidade"
ID_PASTA_DRIVE = "1pz1Qq3SN0SeMyZK4RIPsEi-DpLIRbdKn"
ARQUIVO_PDF_TEMP = "boletim_historico_temp.pdf"

# --- Funções do Google ---
def conectar_google_apis():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
    
    # Procura pelo segredo no ambiente do GitHub Actions
    google_creds_json = os.getenv('GOOGLE_CREDS')
    
    if google_creds_json:
        # Se encontrou o segredo, usa ele
        creds_dict = json.loads(google_creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        # Se não encontrou (rodando localmente), usa o arquivo
        creds = Credentials.from_service_account_file(NOME_ARQUIVO_CREDENCIAL, scopes=scopes)
    
    sheet_client = gspread.authorize(creds)
    spreadsheet = sheet_client.open(NOME_PLANILHA)
    sheet = spreadsheet.worksheet(NOME_PAGINA)
    
    drive_service = build('drive', 'v3', credentials=creds)
    return sheet, drive_service

def obter_boletins_existentes(sheet):
    try:
        return set(sheet.col_values(8)[1:])
    except gspread.exceptions.APIError as e:
        print(f"Erro de API ao buscar colunas: {e}. Verifique se a planilha/página existe.")
        return set()

def adicionar_dados_planilha(sheet, df):
    dados_para_adicionar = df.fillna('').astype(str).values.tolist()
    if dados_para_adicionar:
        sheet.append_rows(dados_para_adicionar, value_input_option="USER_ENTERED")

# --- Lógica Principal ---
def main():
    print("Iniciando scraper histórico...")
    
    try:
        sheet, drive_service = conectar_google_apis()
        boletins_existentes = obter_boletins_existentes(sheet)
        print(f"Encontrados {len(boletins_existentes)} boletins na planilha.")
    except Exception as e:
        print(f"Erro ao conectar com as APIs do Google: {e}")
        return

    try:
        query = f"'{ID_PASTA_DRIVE}' in parents and name contains 'FORTALEZA' and mimeType='application/pdf'"
        
        response = drive_service.files().list(
            q=query,
            pageSize=1000,
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = response.get('files', [])
        print(f"Encontrados {len(files)} boletins de Fortaleza no Google Drive.")

    except Exception as e:
        print(f"Erro ao listar arquivos do Google Drive: {e}")
        return

    novos_boletins_processados = 0
    for file in sorted(files, key=lambda x: x['name']):
        try:
            request = drive_service.files().get_media(fileId=file.get('id'))
            fh = io.FileIO(ARQUIVO_PDF_TEMP, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                _, done = downloader.next_chunk()
            
            numero_boletim, _ = extrair_metadados_pdf(ARQUIVO_PDF_TEMP)
            if not numero_boletim:
                print(f"Não foi possível ler metadados do arquivo: {file.get('name')}. Pulando.")
                continue

            if numero_boletim in boletins_existentes:
                continue
            
            print(f"Processando novo boletim: {numero_boletim} ({file.get('name')})")
            link_drive = f"https://drive.google.com/file/d/{file.get('id')}/view"
            df_novo = processar_pdf_completo(ARQUIVO_PDF_TEMP, link_boletim=link_drive)
            
            if not df_novo.empty:
                adicionar_dados_planilha(sheet, df_novo)
                boletins_existentes.add(numero_boletim)
                novos_boletins_processados += 1
                print(f"  -> Adicionado com sucesso.")
            
        except Exception as e:
            print(f"  -> Erro inesperado ao processar o arquivo {file.get('name')}: {e}")
        finally:
            if os.path.exists(ARQUIVO_PDF_TEMP):
                os.remove(ARQUIVO_PDF_TEMP)

    print(f"\nScraper histórico finalizado. Total de novos boletins processados: {novos_boletins_processados}.")

if __name__ == "__main__":
    main()