import os
import pandas as pd

# Diretório de entrada e saída
diretorio_entrada_bdr = 'E:\\progamação\\vs code\\Bovdb\\tabela_dates\\base\\j2\\geral_j2\\j2_bdr_arquives\\j2_bdr'
diretorio_saida_bdr = 'E:\\progamação\\vs code\\Bovdb\\tabela_dates\\base\\j2\\geral_coluns_edit\\j2_edit_coluns_bdr'

# Verifica se o diretório de saída existe, senão cria
if not os.path.exists(diretorio_saida_bdr):
    os.makedirs(diretorio_saida_bdr)

# Para cada arquivo no diretório de entrada
for arquivo in os.listdir(diretorio_entrada_bdr):
    if arquivo.startswith("combined") and arquivo.endswith(".csv"):
        caminho_arquivo_entrada = os.path.join(diretorio_entrada_bdr, arquivo)
        caminho_arquivo_saida = os.path.join(diretorio_saida_bdr, f'{arquivo.split(".")[0]}_editado.csv')
        
        # Verifica se o arquivo editado já existe no diretório de saída
        if os.path.exists(caminho_arquivo_saida):
            print("Arquivo editado já existe. Pulando para o próximo arquivo:", caminho_arquivo_saida)
            continue
        
        # Lê o arquivo CSV
        df = pd.read_csv(caminho_arquivo_entrada)
        
        # Seleciona apenas as colunas desejadas
        df_editado = df[['CodigoInstrumento', 'Assit', 'Options_actions', 'Options', 'Codisi', 'Company']]
        
        # Renomeia as colunas "CodigoInstrumento" para "Ticker" e "Options" para "Type"
        df_editado = df_editado.rename(columns={'CodigoInstrumento': 'Ticker', 'Options': 'type'})
        
        # Salva o arquivo editado no diretório de saída
        df_editado.to_csv(caminho_arquivo_saida, index=False)
        print("Arquivo editado e salvo:", caminho_arquivo_saida)
