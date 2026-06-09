"""Módulo responsável por gerenciar o armazenamento e os cálculos de custos no DuckDB."""

import duckdb
import pandas as pd

class DataManager:
    """Classe responsável por gerenciar a conexão com o banco de dados DuckDB e operações relacionadas."""

    def __init__(self, db_path='data/aanc_gestamp.db'):
        try:
            self.conn = duckdb.connect(db_path)
            print(f"Conexão estabelecida com o banco de dados: {db_path}")
        except Exception as e:
            raise Exception(f"Erro ao conectar ao banco de dados: {str(e)}")

    def inicializar_tabelas(self):
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS headcount AS
                SELECT * FROM read_csv_auto('data/headcount.csv')
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS premissas AS
                SELECT * FROM read_csv_auto('data/premissas_plantas.csv')
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS referencias_sindicais AS
                SELECT * FROM read_csv_auto('data/Relação_sindicato_planta.csv')
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS benchmark_mercado AS
                SELECT * FROM read_csv_auto('data/benchmark_mercado.csv')
            """)
        except FileNotFoundError as e:
            raise Exception(f"Arquivo CSV não encontrado: {str(e)}. Verifique se os arquivos estão na pasta 'data'.")
        except Exception as e:
            raise Exception(f"Erro ao inicializar tabelas: {str(e)}. Verifique os arquivos CSV e tente novamente.")

    def executar_consulta(self, sql_query):
        try:
            result = self.conn.execute(sql_query)
            return result.fetchdf()
        except Exception as e:
            error_msg = str(e)
            if "no such table" in error_msg.lower():
                raise Exception("Erro: A tabela especificada na consulta não existe. Verifique se as tabelas foram inicializadas corretamente.")
            if "syntax error" in error_msg.lower() or "parse error" in error_msg.lower():
                raise Exception("Erro: A consulta SQL contém um erro de sintaxe. Verifique a estrutura da consulta e tente novamente.")
            raise Exception(f"Erro ao executar consulta SQL: {error_msg}. Verifique a consulta e tente novamente.")

    def simular_cenario_completo(self, planta_id, pct_salario, pct_va, pct_plr, pct_he_adicional):
        try:
            query = f"""
                SELECT
                    h.salario_atual,
                    h.valor_va_atual,
                    h.plr_alvo_atual,
                    p.rat_fap,
                    p.fgts,
                    p.inss_patronal,
                    p.terceiros,
                    p.provisao_ferias,
                    p.provisao_13,
                    p.perc_he_medio
                FROM headcount h
                JOIN premissas p ON h.planta = p.planta
                WHERE h.planta = '{planta_id}'
            """
            df = self.executar_consulta(query)
            if df.empty:
                raise Exception(f"Planta '{planta_id}' não encontrada ou sem dados para simulação.")

            df = df.fillna(0)
            custo_atual_total = 0.0
            custo_projetado_total = 0.0
            total_va_atual = 0.0
            total_plr_atual = 0.0
            novo_va_total = 0.0
            novo_plr_total = 0.0
            total_salario_atual_encargos = 0.0
            total_salario_projetado_encargos = 0.0

            for _, row in df.iterrows():
                salario_atual = float(row['salario_atual'])
                va_atual = float(row['valor_va_atual'])
                plr_atual = float(row['plr_alvo_atual'])
                rat_fap = float(row['rat_fap'])
                fgts = float(row['fgts'])
                inss_patronal = float(row['inss_patronal'])
                terceiros = float(row['terceiros'])
                provisao_ferias = float(row['provisao_ferias'])
                provisao_13 = float(row['provisao_13'])
                perc_he_medio = float(row['perc_he_medio'])

                base_atual = salario_atual + (perc_he_medio * salario_atual * 1.2)
                com_provisoes_atual = base_atual * (1 + provisao_ferias + provisao_13)
                custo_salario_atual = com_provisoes_atual * (1 + rat_fap + fgts + inss_patronal + terceiros)
                custo_va_atual = va_atual
                custo_plr_atual = plr_atual
                custo_atual_total += custo_salario_atual + custo_va_atual + custo_plr_atual

                salario_novo = salario_atual * (1 + pct_salario / 100)
                va_novo = va_atual * (1 + pct_va / 100)
                plr_novo = plr_atual * (1 + pct_plr / 100)
                perc_he_novo = perc_he_medio * (1 + pct_he_adicional / 100)

                base_projetado = salario_novo + (perc_he_novo * salario_novo * 1.2)
                com_provisoes_projetado = base_projetado * (1 + provisao_ferias + provisao_13)
                custo_salario_projetado = com_provisoes_projetado * (1 + rat_fap + fgts + inss_patronal + terceiros)
                custo_va_projetado = va_novo
                custo_plr_projetado = plr_novo
                custo_projetado_total += custo_salario_projetado + custo_va_projetado + custo_plr_projetado

                total_va_atual += va_atual
                total_plr_atual += plr_atual
                novo_va_total += va_novo
                novo_plr_total += plr_novo
                total_salario_atual_encargos += custo_salario_atual
                total_salario_projetado_encargos += custo_salario_projetado

            salario_atual_anual = total_salario_atual_encargos * 12
            salario_projetado_anual = total_salario_projetado_encargos * 12
            va_atual_anual = total_va_atual * 12
            va_projetado_anual = novo_va_total * 12
            plr_atual_anual = total_plr_atual
            plr_projetado_anual = novo_plr_total
            custo_atual_anual = salario_atual_anual + va_atual_anual + plr_atual_anual
            custo_projetado_anual = salario_projetado_anual + va_projetado_anual + plr_projetado_anual
            impacto_anual = custo_projetado_anual - custo_atual_anual

            return {
                'Custo Atual': round(custo_atual_anual, 2),
                'Novo Custo Projetado': round(custo_projetado_anual, 2),
                'Impacto Total Empresa': round(impacto_anual, 2),
                'Impacto Anual Empresa': round(impacto_anual, 2),
                'Detalhes': {
                    'Custo Salário Atual Anual': round(salario_atual_anual, 2),
                    'Custo Salário Projetado Anual': round(salario_projetado_anual, 2),
                    'Custo VA Atual Anual': round(va_atual_anual, 2),
                    'Custo VA Projetado Anual': round(va_projetado_anual, 2),
                    'Custo PLR Atual Anual': round(plr_atual_anual, 2),
                    'Salário Base Atual': round(df['salario_atual'].sum(), 2),
                    'HE Atual': round((df['salario_atual'] * df['perc_he_medio'] * 1.2).sum(), 2),
                    'DSR Atual': 0.0,
                    'VA Atual': round(total_va_atual, 2),
                    'PLR Atual': round(total_plr_atual, 2),
                    'Novo Salário': round(df['salario_atual'].sum() * (1 + pct_salario / 100), 2),
                    'Novo HE': round((df['salario_atual'] * (1 + pct_salario / 100) * (df['perc_he_medio'] * (1 + pct_he_adicional / 100)) * 1.2).sum(), 2),
                    'DSR Projetado': 0.0,
                    'Novo VA': round(novo_va_total, 2),
                    'Novo PLR': round(novo_plr_total, 2)
                }
            }
        except Exception as e:
            raise Exception(f"Erro ao simular cenário para a planta {planta_id}: {str(e)}")

    def obter_benchmark(self, planta_id):
        try:
            df = pd.read_csv('data/benchmark_mercado.csv', sep=';', quotechar='"')
            df_filtrado = df[df['Filial'] == planta_id]
            if df_filtrado.empty:
                return {'message': f"Nenhum dado de benchmark encontrado para a planta '{planta_id}'."}
            return df_filtrado
        except Exception as e:
            return {'message': f"Erro ao obter benchmark para a planta {planta_id}: {str(e)}"}

    def obter_documentos_por_planta(self, planta_id):
        try:
            query = f"SELECT \"Documento Referência\" FROM referencias_sindicais WHERE Filial = '{planta_id}'"
            df = self.executar_consulta(query)
            return df['Documento Referência'].tolist()
        except Exception as e:
            raise Exception(f"Erro ao obter documentos para a planta {planta_id}: {str(e)}")

DatabaseManager = DataManager
