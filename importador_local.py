# importador_local.py (VERSÃO FINAL E DEFINITIVA)

import os
import pandas as pd
from core_parser import processar_pdf_completo

# --- CONFIGURAÇÃO ---
# Coloque aqui o caminho para a pasta onde você extraiu todos os PDFs
PASTA_DOS_PDFS = r"C:\Users\ayala\Desktop\projeto-balneabilidade\pdfs_historicos" 
ARQUIVO_SAIDA_CSV = "historico_completo.csv"

def main():
    print(f"Iniciando importador local da pasta: {PASTA_DOS_PDFS}")
    
    lista_dfs = []
    
    arquivos_pdf = [f for f in os.listdir(PASTA_DOS_PDFS) if f.lower().endswith('.pdf')]
    total_arquivos = len(arquivos_pdf)
    print(f"Encontrados {total_arquivos} arquivos PDF para processar.")

    for i, nome_arquivo in enumerate(arquivos_pdf):
        caminho_completo = os.path.join(PASTA_DOS_PDFS, nome_arquivo)
        print(f"Processando arquivo {i+1}/{total_arquivos}: {nome_arquivo}...")
        
        df_pdf = processar_pdf_completo(caminho_completo)
        
        if not df_pdf.empty:
            lista_dfs.append(df_pdf)
            print(f"  -> Sucesso! {len(df_pdf)} linhas extraídas.")
        else:
            print(f"  -> Aviso: Nenhum dado extraído de {nome_arquivo}.")

    if not lista_dfs:
        print("Nenhum dado foi processado. Encerrando.")
        return
        
    print("\nJuntando todos os dados...")
    df_final = pd.concat(lista_dfs, ignore_index=True)
    
    # --- CORREÇÃO APLICADA AQUI ---
    # Forçamos a conversão das colunas para texto com 15 casas decimais.
    # Isso impede que o pandas use a formatação de localidade do sistema (com separador de milhar).
    df_final['latitude'] = df_final['latitude'].apply(lambda x: f'{x:.15f}' if pd.notnull(x) else '')
    df_final['longitude'] = df_final['longitude'].apply(lambda x: f'{x:.15f}' if pd.notnull(x) else '')
    
    # Salvamos o resultado no CSV. O parâmetro 'decimal' aqui se torna redundante para as coordenadas, mas o mantemos por segurança.
    df_final.to_csv(ARQUIVO_SAIDA_CSV, index=False, encoding='utf-8-sig', decimal='.')
    
    print("-" * 50)
    print("PROCESSO FINALIZADO COM SUCESSO!")
    print(f"Total de {len(df_final)} linhas salvas no arquivo '{ARQUIVO_SAIDA_CSV}'.")
    print("Agora você pode importar este arquivo para o Google Sheets.")

if __name__ == "__main__":
    main()