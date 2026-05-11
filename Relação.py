import duckdb

# 1. Criando (ou conectando) ao arquivo do banco de dados
con = duckdb.connect(database='aanc_database.db')

# 2. Carregando os CSVs para tabelas do DuckDB
# O DuckDB lê os CSVs e já cria as tabelas automaticamente com os tipos de dados corretos
con.execute("CREATE TABLE IF NOT EXISTS headcount AS SELECT * FROM read_csv_auto('headcount.csv')")
con.execute("CREATE TABLE IF NOT EXISTS premissas AS SELECT * FROM read_csv_auto('premissas_plantas.csv')")

print("✅ Tabelas criadas e dados carregados com sucesso!")

# 3. Teste de Relacionamento (JOIN)
# Vamos calcular o custo total mensal de Salário + Combustível para a planta G1 (SJP)
# Note como unimos (JOIN) a tabela de funcionários com a de premissas daquela planta
query_teste = """
    SELECT 
        h.planta,
        COUNT(h.id_funcionario) as total_colaboradores,
        SUM(h.salario_atual) as massa_salarial,
        SUM(p.ajuda_combustivel_planta) as custo_total_combustivel
    FROM headcount h
    JOIN premissas p ON h.planta = p.planta
    WHERE h.planta = 'G1'
    GROUP BY h.planta
"""

resultado = con.execute(query_teste).df()
print("\n--- Teste de Validação de Dados ---")
print(resultado)

con.close()