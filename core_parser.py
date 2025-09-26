import pdfplumber
import camelot
import pandas as pd
import unicodedata
from datetime import datetime, timedelta
from coordenadas import COORDENADAS_POR_CODIGO

# --- Funções Auxiliares (muitas do seu script original) ---

def extract_point_code(nome: str) -> str:
    return (nome[:3] or "").strip().upper()

def expand_periodo(periodo_str: str):
    try:
        inicio_str, fim_str = [p.strip() for p in periodo_str.split("a")]
        dt_inicio = datetime.strptime(inicio_str, "%d/%m/%Y")
        dt_fim = datetime.strptime(fim_str, "%d/%m/%Y")
        dias = []
        atual = dt_inicio
        while atual <= dt_fim:
            dias.append(atual.strftime("%Y-%m-%d"))
            atual += timedelta(days=1)
        return dias
    except Exception:
        return []

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def classify_zona(nome: str) -> str:
    n = strip_accents((nome or "").lower())
    leste_kw = ["futuro", "caca e pesca", "abreulandia", "sabiaguaba", "titanzinho"]
    centro_kw = ["iracema", "meireles", "mucuripe", "volta da jurema", "beira mar", "estressados"]
    oeste_kw = ["barra do ceara", "pirambu", "cristo redentor", "leste oeste", "formosa", "colonia"]
    if any(k in n for k in leste_kw): return "Leste"
    if any(k in n for k in centro_kw): return "Centro"
    if any(k in n for k in oeste_kw): return "Oeste"
    return "Desconhecida"

def clean_status_token(tok: str) -> str:
    tok = tok.strip().upper()
    return tok if tok in ("P", "I") else ""

def is_noise_row(nome: str, status: str) -> bool:
    txt = f"{str(nome)} {str(status)}".lower()
    noise_terms = ["nome", "status", "trecho", "ponto", "boletim", "semace"]
    if len(txt.strip()) < 3: return True
    return any(term in txt for term in noise_terms)

# --- Funções Principais de Processamento ---

def extrair_metadados_pdf(caminho_pdf):
    """Extrai apenas os metadados (número do boletim e período) de um PDF."""
    try:
        with pdfplumber.open(caminho_pdf) as pdf:
            texto_pg1 = pdf.pages[0].extract_text()
            texto_pg1 = " ".join(texto_pg1.split())
            
            numero_boletim = ""
            periodo = ""

            if "Nº" in texto_pg1 and "Período:" in texto_pg1:
                bol_index = texto_pg1.find("Nº")
                per_index = texto_pg1.find("Período:", bol_index)
                
                numero_boletim = texto_pg1[bol_index + 2:per_index].strip().replace("o", "")

                tipos_index = texto_pg1.find("Tipos de amostras:", per_index)
                periodo_raw = texto_pg1[per_index + len("Período:"):tipos_index if tipos_index != -1 else None].strip()
                
                # Tenta extrair um período como "dd/mm/yyyy a dd/mm/yyyy"
                partes = periodo_raw.replace(" de ", "/").split()
                if len(partes) >= 3 and partes[1].lower() == 'a':
                    periodo = f"{partes[0]} a {partes[2]}"
                else:
                    periodo = periodo_raw # Fallback

                return numero_boletim, periodo
    except Exception as e:
        print(f"Erro ao extrair metadados: {e}")
        return None, None
    return None, None


def processar_pdf_completo(caminho_pdf, link_boletim=""):
    """
    Processa um arquivo PDF de boletim e retorna um DataFrame limpo e estruturado.
    """
    numero_boletim, periodo = extrair_metadados_pdf(caminho_pdf)
    if not numero_boletim or not periodo:
        print(f"Não foi possível extrair metadados de {caminho_pdf}. Pulando arquivo.")
        return pd.DataFrame()

    # Extração das tabelas com Camelot
    try:
        tables = camelot.read_pdf(caminho_pdf, pages="1-end", flavor="stream")
    except Exception as e:
        print(f"Erro ao ler tabelas do PDF {caminho_pdf}: {e}")
        return pd.DataFrame()

    dfs_norm = []
    for t in tables:
        df_raw = t.df.copy()
        if df_raw.shape[1] < 2: continue
        
        df_raw = df_raw.iloc[:, :2]
        df_raw.columns = ["Nome", "Status"]
        linhas = []
        for _, row in df_raw.iterrows():
            nomes = [x.strip() for x in str(row["Nome"]).split("\n") if x.strip()]
            status_tokens = [clean_status_token(x) for x in str(row["Status"]).split("\n")]
            status_tokens = [x for x in status_tokens if x]
            if not nomes or not status_tokens: continue
            
            if len(status_tokens) == 1 and len(nomes) > 1:
                for n in nomes:
                    if not is_noise_row(n, status_tokens[0]):
                        linhas.append({"nome_praia": n, "status_sigla": status_tokens[0]})
            else:
                for n, s in zip(nomes, status_tokens):
                    if not is_noise_row(n, s):
                        linhas.append({"nome_praia": n, "status_sigla": s})
        if linhas:
            dfs_norm.append(pd.DataFrame(linhas))

    if not dfs_norm:
        print(f"Nenhuma tabela válida encontrada em {caminho_pdf}.")
        return pd.DataFrame()

    df = pd.concat(dfs_norm, ignore_index=True)
    df["nome_praia"] = df["nome_praia"].apply(lambda x: " ".join(x.split()))
    df = df.drop_duplicates(subset=["nome_praia"]).reset_index(drop=True)

    # Adicionar e formatar colunas
    df["id_ponto"] = df["nome_praia"].apply(extract_point_code)
    df["zona"] = df["nome_praia"].apply(classify_zona)
    df["status"] = df["status_sigla"].map({"P": "Própria para banho", "I": "Imprópria para banho"})
    
    # Separar coordenadas em latitude e longitude
    coords_series = df["id_ponto"].map(COORDENADAS_POR_CODIGO)
    coords_df = coords_series.str.split(",", expand=True)
    df["latitude"] = pd.to_numeric(coords_df[0], errors='coerce')
    df["longitude"] = pd.to_numeric(coords_df[1], errors='coerce')

    df["numero_boletim"] = numero_boletim
    df["link_boletim"] = link_boletim
    df["data_extracao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Expandir o período para ter uma linha por dia
    dias_periodo = expand_periodo(periodo)
    if not dias_periodo:
        print(f"Não foi possível expandir o período '{periodo}' para o boletim {numero_boletim}")
        return pd.DataFrame()

    df_final = pd.DataFrame()
    for dia in dias_periodo:
        df_dia = df.copy()
        df_dia["data_coleta"] = dia
        df_final = pd.concat([df_final, df_dia], ignore_index=True)

    # Ordenar e selecionar colunas finais
    colunas_finais = [
        "id_ponto", "data_coleta", "nome_praia", "zona", "status",
        "latitude", "longitude", "numero_boletim", "link_boletim", "data_extracao"
    ]
    df_final = df_final.reindex(columns=colunas_finais)
    return df_final