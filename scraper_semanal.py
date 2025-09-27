# scraper_semanal.py (VERSÃO FINAL PARA GITHUB ACTIONS)

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import gspread
import pandas as pd
import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from core_parser import processar_pdf_completo, extrair_metadados_pdf

# --- Configurações ---
URL_BASE = "https://www.semace.ce.gov.br/boletim-de-balneabilidade/"
NOME_ARQUIVO_CREDENCIAL = "credentials.json"
NOME_PLANILHA = "balneabilidade_fortaleza" # <<<!!! TROQUE PELO NOME EXATO DA SUA PLANILHA !!!>>>
NOME_PAGINA = "DadosBalneabilidade"
ARQUIVO_PDF_TEMP = "boletim_semanal_temp.pdf"

# --- Funções do Google Sheets ---
def conectar_google_apis():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
    google_creds_json = os.getenv('GOOGLE_CREDS')
    if google_creds_json:
        creds_dict = json.loads(google_creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(NOME_ARQUIVO_CREDENCIAL, scopes=scopes)
    
    sheet_client = gspread.authorize(creds)
    spreadsheet = sheet_client.open(NOME_PLANILHA)
    sheet = spreadsheet.worksheet(NOME_PAGINA)
    return sheet, None # Retorna None para drive_service, já que não é usado aqui

def obter_boletins_existentes(sheet):
    try:
        return set(sheet.col_values(8)[1:])
    except gspread.exceptions.APIError as e:
        print(f"Erro de API ao buscar colunas: {e}.")
        return set()

def adicionar_dados_planilha(sheet, df):
    dados_para_adicionar = df.fillna('').astype(str).values.tolist()
    if dados_para_adicionar:
        sheet.append_rows(dados_para_adicionar, value_input_option="USER_ENTERED")

# --- Lógica Principal ---
def main():
    print("Iniciando scraper semanal...")
    try:
        sheet, _ = conectar_google_apis()
        boletins_existentes = obter_boletins_existentes(sheet)
        print(f"Encontrados {len(boletins_existentes)} boletins na planilha.")
    except Exception as e:
        print(f"Erro ao conectar com o Google Sheets: {e}")
        return

    try:
        res = requests.get(URL_BASE)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        links_boletim = [a['href'] for a in soup.find_all('a', href=True) if "Boletim das Praias de Fortaleza" in a.get_text()]
        if not links_boletim:
            print("Nenhum link de boletim de Fortaleza encontrado na página.")
            return
        ultimo_boletim_url = urljoin(URL_BASE, links_boletim[0])
        print(f"URL do último boletim: {ultimo_boletim_url}")
    except Exception as e:
        print(f"Erro ao buscar link no site da SEMACE: {e}")
        return

    try:
        res_pdf = requests.get(ultimo_boletim_url, stream=True)
        res_pdf.raise_for_status()
        with open(ARQUIVO_PDF_TEMP, "wb") as f:
            f.write(res_pdf.content)
    except Exception as e:
        print(f"Erro ao baixar o PDF: {e}")
        return

    numero_boletim, _ = extrair_metadados_pdf(ARQUIVO_PDF_TEMP)
    if not numero_boletim or numero_boletim in boletins_existentes:
        if numero_boletim:
            print(f"Boletim {numero_boletim} já existe na planilha. Encerrando.")
        else:
            print("Não foi possível extrair o número do boletim do PDF baixado.")
        if os.path.exists(ARQUIVO_PDF_TEMP):
            os.remove(ARQUIVO_PDF_TEMP)
        return

    print(f"Boletim {numero_boletim} é novo. Processando...")
    df_novo = processar_pdf_completo(ARQUIVO_PDF_TEMP, link_boletim=ultimo_boletim_url)

    if not df_novo.empty:
        adicionar_dados_planilha(sheet, df_novo)
        print(f"{len(df_novo)} linhas adicionadas à planilha com sucesso.")
    else:
        print("O processamento do PDF não retornou dados.")

    if os.path.exists(ARQUIVO_PDF_TEMP):
        os.remove(ARQUIVO_PDF_TEMP)
    print("Scraper semanal finalizado.")

if __name__ == "__main__":
    main()