import sqlite3

# Caminho para o banco de dados
db_path = r"E:\progamação\vs code\Bovdb\tabela_dates\Database_define.db"

# Conectar ao banco de dados
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Atualizar as colunas 'volume', 'business' e 'amount_stock' para NULL
cursor.execute("UPDATE price5 SET volume = NULL, business = NULL, amount_stock = NULL")

# Salvar as mudanças e fechar a conexão
conn.commit()
conn.close()

print("As informações das colunas 'volume', 'business' e 'amount_stock' foram excluídas.")
