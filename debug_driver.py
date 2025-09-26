# debug_drive.py - Script de Teste de Conexão com o Google Drive

import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Configurações ---
NOME_ARQUIVO_CREDENCIAL = "credentials.json"
ID_PASTA_DRIVE = "1pz1Qq3SN0SeMyZK4RIPsEi-DpLIRbdKn" # ID da pasta pública da SEMACE

print("--- INICIANDO TESTE DE DIAGNÓSTICO DO GOOGLE DRIVE ---")

# 1. Tentar conectar e autenticar
try:
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = Credentials.from_service_account_file(NOME_ARQUIVO_CREDENCIAL, scopes=scopes)
    drive_service = build('drive', 'v3', credentials=creds)
    print("✅ PASSO 1: Autenticação com a API do Google Drive bem-sucedida.")
except Exception as e:
    print(f"❌ FALHA no PASSO 1: Não foi possível autenticar. Erro: {e}")
    exit()

# 2. Tentar obter informações da pasta específica
try:
    print(f"\n--- TENTANDO ACESSAR A PASTA COM ID: {ID_PASTA_DRIVE} ---")
    # Usamos fields='*' para pedir todas as informações possíveis sobre a pasta
    # supportsAllDrives=True é crucial para pastas compartilhadas
    folder_metadata = drive_service.files().get(
        fileId=ID_PASTA_DRIVE, 
        fields='id, name, webViewLink, parents', 
        supportsAllDrives=True
    ).execute()

    print("✅ PASSO 2: SUCESSO! O robô conseguiu 'enxergar' a pasta.")
    print("   -> Informações da pasta:")
    print(f"   - Nome: {folder_metadata.get('name')}")
    print(f"   - ID: {folder_metadata.get('id')}")
    print(f"   - Link: {folder_metadata.get('webViewLink')}")

except HttpError as e:
    print(f"❌ FALHA no PASSO 2: O robô NÃO conseguiu enxergar a pasta.")
    if e.resp.status == 404:
        print("   -> Erro: 'File not found' (Arquivo não encontrado).")
        print("   -> Diagnóstico: A Conta de Serviço não tem permissão para ver esta pasta, mesmo ela sendo pública.")
    else:
        print(f"   -> Erro Inesperado: {e}")
except Exception as e:
    print(f"❌ FALHA no PASSO 2: Ocorreu um erro geral.")
    print(f"   -> Detalhes: {e}")

print("\n--- TESTE DE DIAGNÓSTICO FINALIZADO ---")