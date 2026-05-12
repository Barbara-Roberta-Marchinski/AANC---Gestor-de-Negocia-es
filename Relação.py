import duckdb
import pandas as pd
import os

# 1. Configuração de caminhos
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
caminho_db = os.path.join(diretorio_atual, 'aanc_database.db')
pasta_data = os.path.join(diretorio_atual, 'data')

# Conecta ao banco de dados
con = duckdb.connect(database=caminho_db)

def carregar_na_marra(nome_arquivo, nome_tabela):
    caminho = os.path.join(pasta_data, nome_arquivo)
    
    # Forçamos o separador ';' e o encoding que limpa o lixo do Excel
    # Adicionamos 'on_bad_lines' para ele não travar se houver alguma linha estranha
    df = pd.read_csv(caminho, sep=';', encoding='utf-8-sig', on_bad_lines='skip')
    
    # Limpeza total dos nomes das colunas
    # Remove aspas, remove o caractere \ufeff e remove espaços
    df.columns = [str(c).replace('"', '').replace('\ufeff', '').strip() for c in df.columns]
    
    # Se por algum motivo o pandas leu tudo em uma coluna só, vamos forçar a separação manual
    if len(df.columns) == 1 and ';' in df.columns[0]:
        coluna_unica = df.columns[0]
        novas_colunas = coluna_unica.split(';')
        df[novas_colunas] = df[coluna_unica].str.split(';', expand=True)
        df = df.drop(columns=[coluna_unica])
    
    # Garante que a primeira coluna se chame 'Filial'
    df.rename(columns={df.columns[0]: 'Filial'}, inplace=True)
    
    # Salva no DuckDB
    con.register('df_temp', df)
    con.execute(f"CREATE OR REPLACE TABLE {nome_tabela} AS SELECT * FROM df_temp")
    con.unregister('df_temp')
    print(f"✅ Tabela {nome_tabela} carregada. Colunas: {list(df.columns[:3])}...")

try:
    print("--- Iniciando Operação de Resgate de Dados ---")
    
    carregar_na_marra('benchmark_mercado.csv', 'benchmark')
    carregar_na_marra('mapeamento_sinonimos.csv', 'sinonimos')
    carregar_na_marra('headcount.csv', 'headcount')
    carregar_na_marra('premissas_plantas.csv', 'premissas')

    print("\n--- Validando dados da G3 ---")
    # Teste de consulta
    res = con.execute("SELECT * FROM benchmark WHERE Filial = 'G3'").df()
    
    if not res.empty:
        print("🚀 CONSEGUIMOS! Veja os dados da G3 abaixo:")
        print(res)
    else:
        print("⚠️ A tabela foi criada, mas não encontrei 'G3' nos dados. Verifique o CSV.")

except Exception as e:
    print(f"\n❌ Erro: {e}")

finally:
    con.close()