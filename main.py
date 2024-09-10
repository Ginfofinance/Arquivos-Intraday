import os
import pandas as pd
import sqlite3

# Diretório contendo os arquivos CSV
csv_directory = r"C:\bovdb\volume\volume_shares"

# Função para ler e processar cada arquivo CSV
def read_and_process_csv(file):
    df = pd.read_csv(file)
    df['HoraFechamento'] = df['HoraFechamento'].apply(lambda x: f"{str(x).zfill(6)[:2]}:{str(x).zfill(6)[2:4]}:{str(x).zfill(6)[4:6]}")
    return df

# Conectar ao banco de dados
db_file = r"C:\bovdb\volume\price_volume.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Função para calcular os valores do intervalo
def process_interval(data):
    volume = float(data['Volume'].sum())
    business = int(len(data))
    amount_stock = int(data['QuantidadeNegociada'].sum())
    return volume, business, amount_stock

# Lista de intervalos de 5 minutos (defina os intervalos apropriados aqui)
intervals = [

]

# Listar todos os arquivos CSV no diretório
for file_name in os.listdir(csv_directory):
    if file_name.endswith('.csv'):
        file_path = os.path.join(csv_directory, file_name)
        
        # Ler e processar o arquivo CSV
        df = read_and_process_csv(file_path)

        # Obter a lista de tickers únicos
        tickers = df['CodigoInstrumento'].unique()

        # Processar dados para cada ticker e intervalo
        for ticker in tickers:
            # Obter id_ticker correspondente ao ticker
            cursor.execute("SELECT id_ticker FROM Ticker WHERE ticker = ?", (ticker,))
            result = cursor.fetchone()
            
            if result is None:
                print(f"Ticker {ticker} não encontrado na tabela Ticker.")
                continue
            
            id_ticker = result[0]
            
            # Filtrar os dados para o ticker atual
            df_filtered = df[df['CodigoInstrumento'] == ticker].copy()
            
            # Processar dados para cada intervalo
            for interval_start, interval_end in intervals:
                interval_data = df_filtered[(df_filtered['HoraFechamento'] >= interval_start) & (df_filtered['HoraFechamento'] < interval_end)]
                
                # Se houver dados no intervalo, processar e inserir no banco
                if not interval_data.empty:
                    volume, business, amount_stock = process_interval(interval_data)
                    
                    # Excluir dados existentes para o mesmo id_ticker, date e time
                    cursor.execute("""
                        DELETE FROM Price5 WHERE id_ticker = ? AND date = ? AND time = ?
                    """, (id_ticker, interval_data.iloc[0]['DataReferencia'], interval_start)) 
                    
                    # Inserir dados novos na tabela Price5
                    cursor.execute("""
                        INSERT INTO Price5 (id_ticker, date, time, volume, business, amount_stock)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (id_ticker, interval_data.iloc[0]['DataReferencia'], interval_start, volume, business, amount_stock))

                    conn.commit()

# Fechar conexão com o banco de dados
conn.close()
