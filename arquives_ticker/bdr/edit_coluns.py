import sqlite3
import pandas as pd
from fuzzywuzzy import fuzz
import os

# Caminho para o banco de dados
db_path = r"C:\bovdb\DataBase copy.db"

# Caminhos dos arquivos CSV
arquivos_csv = [
    "E:\\progamação\\vs code\\Bovdb\\tabela_dates\\base\\j2\\geral_coluns_edit\\j2_edit_coluns_bdr\\combined_20240130_fmt_editado.csv",
    "E:\\progamação\\vs code\\Bovdb\\tabela_dates\\base\\j2\\geral_coluns_edit\\j2_edit_coluns_bdr\\combined_20240228_fmt_editado.csv",
    "E:\\progamação\\vs code\\Bovdb\\tabela_dates\\base\\j2\\geral_coluns_edit\\j2_edit_coluns_bdr\\combined_20240627_fmt_editado.csv"
]

# Conectando ao banco de dados
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Funções auxiliares
def obter_id_company(company_name):
    cursor.execute("SELECT id_company, company FROM Company")
    companies = cursor.fetchall()
    for id_company, company in companies:
        if fuzz.ratio(company_name.lower(), company.lower()) > 90:
            return id_company, company
    return None, None

def obter_id_type(type_name):
    cursor.execute("SELECT id_type FROM Type WHERE ds_type = ?", (type_name,))
    result = cursor.fetchone()
    return result[0] if result else None

def obter_id_options(options_name):
    cursor.execute("SELECT id_options FROM Options WHERE ds_options = ?", (options_name,))
    result = cursor.fetchone()
    return result[0] if result else None

def obter_id_ticker(ticker):
    cursor.execute("SELECT id_ticker, id_type FROM Ticker WHERE ticker = ?", (ticker,))
    result = cursor.fetchone()
    return (result[0], result[1]) if result else (None, None)

def inserir_company(company_name):
    cursor.execute("INSERT INTO Company (company) VALUES (?)", (company_name,))
    conn.commit()
    return cursor.lastrowid

def inserir_ticker(ticker, id_company, id_type, id_options, codisi):
    cursor.execute("INSERT INTO Ticker (ticker, id_company, id_type, id_options, codisi) VALUES (?, ?, ?, ?, ?)",
                   (ticker, id_company, id_type, id_options, codisi))
    conn.commit()
    return cursor.lastrowid

def atualizar_id_type_ticker(id_ticker, id_type):
    cursor.execute("UPDATE Ticker SET id_type = ? WHERE id_ticker = ?", (id_type, id_ticker))
    conn.commit()

# Processando cada arquivo CSV
for arquivo_csv in arquivos_csv:
    df = pd.read_csv(arquivo_csv)
    
    for index, row in df.iterrows():
        ticker = row['Ticker']
        company = row['Company']
        type_name = row['type']
        options_name = row['Options_actions']
        codisi = row['Codisi']
        
        # Verificar se o ticker já existe na tabela Ticker
        id_ticker, current_id_type = obter_id_ticker(ticker)
        
        if id_ticker:
            if current_id_type != obter_id_type(type_name):
                # Atualizar id_type se necessário
                atualizar_id_type_ticker(id_ticker, obter_id_type(type_name))
                print(f"Ticker '{ticker}' atualizado com id_type = {obter_id_type(type_name)}")
            else:
                print(f"Ticker '{ticker}' já está na tabela com id_ticker = {id_ticker} e id_type = {current_id_type}")
        else:
            # Verificar se a company já existe na tabela Company
            id_company, existing_company_name = obter_id_company(company)
            if not id_company:
                # Inserir nova empresa apenas se o ticker não existir
                id_company = inserir_company(company)
                print(f"Company '{company}' inserida com id_company = {id_company}")
            else:
                print(f"Company '{company}' é considerada similar a '{existing_company_name}' já existente com id_company = {id_company}")
            
            # Obter id_type e id_options
            id_type = obter_id_type(type_name)
            id_options = obter_id_options(options_name)
            
            # Inserir novo ticker
            id_ticker = inserir_ticker(ticker, id_company, id_type, id_options, codisi)
            print(f"Ticker '{ticker}' inserido com id_ticker = {id_ticker}")

# Fechando a conexão
conn.close()
