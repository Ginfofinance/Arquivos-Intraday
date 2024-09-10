import os
import pandas as pd

# Diretórios de entrada e saída
input_dir = r"E:\progamação\vs code\Bovdb\tabela_dates\base\j2\geral_j2\j2_bdr_arquives\j2_bdr"
output_dir = r"C:\bovdb\volume\volume_bdr"

# Verifica se o diretório de saída existe, caso contrário, cria-o
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Processa cada arquivo CSV no diretório de entrada
for filename in os.listdir(input_dir):
    if filename.endswith(".csv"):
        input_file = os.path.join(input_dir, filename)
        output_file = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}_adjusted.csv")
        
        # Ler o arquivo CSV
        df = pd.read_csv(input_file)

        # Converter a coluna 'HoraFechamento' para string para facilitar a manipulação
        df['HoraFechamento'] = df['HoraFechamento'].astype(str)

        # Extrair as partes HHMMSS de 'HoraFechamento'
        df['HoraFechamento_HHMMSS'] = df['HoraFechamento'].str[:6]

        # Criar uma nova coluna que representa o intervalo de 10 segundos
        df['HoraFechamento_10s'] = df['HoraFechamento_HHMMSS'].apply(
            lambda x: x[:4] + str(int(x[4:6]) // 10 * 10).zfill(2)
        )

        # Converter 'PrecoNegocio' de string com vírgula decimal para float
        df['PrecoNegocio'] = df['PrecoNegocio'].str.replace(',', '.').astype(float)

        # Agrupar por 'HoraFechamento_10s' e aplicar as regras para as colunas
        df_grouped = df.groupby(['CodigoInstrumento', 'HoraFechamento_10s']).agg({
            'DataReferencia': 'first',               # Manter a primeira data
            'PrecoNegocio': 'last',                  # Manter o último preço
            'QuantidadeNegociada': 'mean',           # Calcular a média da quantidade negociada
            'CodigoIdentificadorNegocio': 'last',    # Manter o último código do identificador
        }).reset_index()

        # Arredondar 'QuantidadeNegociada' e 'PrecoNegocio'
        df_grouped['QuantidadeNegociada'] = df_grouped['QuantidadeNegociada'].round(1)
        df_grouped['PrecoNegocio'] = df_grouped['PrecoNegocio'].round(2)

        # Calcular o volume para cada linha (PrecoNegocio * QuantidadeNegociada)
        df_grouped['Volume'] = (df_grouped['PrecoNegocio'] * df_grouped['QuantidadeNegociada']).round(1)

        # Renomear a coluna 'HoraFechamento_10s' para 'HoraFechamento'
        df_grouped = df_grouped.rename(columns={'HoraFechamento_10s': 'HoraFechamento'})

        # Salvar o resultado em um novo arquivo CSV
        df_grouped.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Arquivo ajustado salvo em: {output_file}")
