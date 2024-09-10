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
    ("09:00:00", "09:05:00"),
    ("09:05:00", "09:10:00"),
    ("09:10:00", "09:15:00"),
    ("09:15:00", "09:20:00"),
    ("09:20:00", "09:25:00"),
    ("09:25:00", "09:30:00"),
    ("09:30:00", "09:35:00"),
    ("09:35:00", "09:40:00"),
    ("09:40:00", "09:45:00"),
    ("09:45:00", "09:50:00"),
    ("09:50:00", "09:55:00"),
    ("09:55:00", "10:00:00"),
    ("10:00:00", "10:05:00"),
    ("10:05:00", "10:10:00"),
    ("10:10:00", "10:15:00"),
    ("10:15:00", "10:20:00"),
    ("10:20:00", "10:25:00"),
    ("10:25:00", "10:30:00"),
    ("10:30:00", "10:35:00"),
    ("10:35:00", "10:40:00"),
    ("10:40:00", "10:45:00"),
    ("10:45:00", "10:50:00"),
    ("10:50:00", "10:55:00"),
    ("10:55:00", "11:00:00"),
    ("11:00:00", "11:05:00"),
    ("11:05:00", "11:10:00"),
    ("11:10:00", "11:15:00"),
    ("11:15:00", "11:20:00"),
    ("11:20:00", "11:25:00"),
    ("11:25:00", "11:30:00"),
    ("11:30:00", "11:35:00"),
    ("11:35:00", "11:40:00"),
    ("11:40:00", "11:45:00"),
    ("11:45:00", "11:50:00"),
    ("11:50:00", "11:55:00"),
    ("11:55:00", "12:00:00"),
    ("12:00:00", "12:05:00"),
    ("12:05:00", "12:10:00"),
    ("12:10:00", "12:15:00"),
    ("12:15:00", "12:20:00"),
    ("12:20:00", "12:25:00"),
    ("12:25:00", "12:30:00"),
    ("12:30:00", "12:35:00"),
    ("12:35:00", "12:40:00"),
    ("12:40:00", "12:45:00"),
    ("12:45:00", "12:50:00"),
    ("12:50:00", "12:55:00"),
    ("12:55:00", "13:00:00"),
    ("13:00:00", "13:05:00"),
    ("13:05:00", "13:10:00"),
    ("13:10:00", "13:15:00"),
    ("13:15:00", "13:20:00"),
    ("13:20:00", "13:25:00"),
    ("13:25:00", "13:30:00"),
    ("13:30:00", "13:35:00"),
    ("13:35:00", "13:40:00"),
    ("13:40:00", "13:45:00"),
    ("13:45:00", "13:50:00"),
    ("13:50:00", "13:55:00"),
    ("13:55:00", "14:00:00"),
    ("14:00:00", "14:05:00"),
    ("14:05:00", "14:10:00"),
    ("14:10:00", "14:15:00"),
    ("14:15:00", "14:20:00"),
    ("14:20:00", "14:25:00"),
    ("14:25:00", "14:30:00"),
    ("14:30:00", "14:35:00"),
    ("14:35:00", "14:40:00"),
    ("14:40:00", "14:45:00"),
    ("14:45:00", "14:50:00"),
    ("14:50:00", "14:55:00"),
    ("14:55:00", "15:00:00"),
    ("15:00:00", "15:05:00"),
    ("15:05:00", "15:10:00"),
    ("15:10:00", "15:15:00"),
    ("15:15:00", "15:20:00"),
    ("15:20:00", "15:25:00"),
    ("15:25:00", "15:30:00"),
    ("15:30:00", "15:35:00"),
    ("15:35:00", "15:40:00"),
    ("15:40:00", "15:45:00"),
    ("15:45:00", "15:50:00"),
    ("15:50:00", "15:55:00"),
    ("15:55:00", "16:00:00"),
    ("16:00:00", "16:05:00"),
    ("16:05:00", "16:10:00"),
    ("16:10:00", "16:15:00"),
    ("16:15:00", "16:20:00"),
    ("16:20:00", "16:25:00"),
    ("16:25:00", "16:30:00"),
    ("16:30:00", "16:35:00"),
    ("16:35:00", "16:40:00"),
    ("16:40:00", "16:45:00"),
    ("16:45:00", "16:50:00"),
    ("16:50:00", "16:55:00"),
    ("16:55:00", "17:00:00"),
    ("17:00:00", "17:05:00"),
    ("17:05:00", "17:10:00"),
    ("17:10:00", "17:15:00"),
    ("17:15:00", "17:20:00"),
    ("17:20:00", "17:25:00"),
    ("17:25:00", "17:30:00"),
    ("17:30:00", "17:35:00"),
    ("17:35:00", "17:40:00"),
    ("17:40:00", "17:45:00"),
    ("17:45:00", "17:50:00"),
    ("17:50:00", "17:55:00"),
    ("17:55:00", "18:00:00")
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
