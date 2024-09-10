import sqlite3

# Caminho para o banco de dados
db_path = r'C:\bovdb\volume\price_volume copy.db'

# Conectar ao banco de dados
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Excluir a tabela 'price5' se existir
cursor.execute('DROP TABLE IF EXISTS price5')

# Criar a tabela 'price5' com a nova estrutura
create_table_query = '''
CREATE TABLE price5 (
    id_ticker INTEGER,
    date DATE,
    time TIME,
    volume DECIMAL,
    business INTEGER,
    amount_stock INTEGER,
    PRIMARY KEY (id_ticker, date, time),
    FOREIGN KEY (id_ticker) REFERENCES Ticker(id_ticker)
)
'''

cursor.execute(create_table_query)

# Confirmar as alterações e fechar a conexão
conn.commit()
conn.close()

print("Tabela 'price5' recriada com sucesso.")
