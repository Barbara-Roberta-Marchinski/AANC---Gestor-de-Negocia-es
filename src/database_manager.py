"""Módulo responsável por gerenciar o armazenamento e os cálculos de custos no DuckDB."""
import os
import duckdb
import pandas as pd
import os
import pandas as pd

# 1. Descobrir onde o script atual está salvo (ex: C:/.../src/database_manager.py)
caminho_atual = os.path.abspath(__file__)
diretorio_do_script = os.path.dirname(caminho_atual)

# 2. Subir um nível para chegar na raiz do projeto (IA-Factor)
# Se o script estiver em 'src', subimos um nível. Se estiver na raiz, ficamos nela.
if diretorio_do_script.endswith('src'):
    raiz_projeto = os.path.dirname(diretorio_do_script)
else:
    raiz_projeto = diretorio_do_script

# 3. Definir o caminho exato para a pasta 'data'
pasta_data = os.path.join(raiz_projeto, 'data')

print(f"📍 Pasta de dados identificada em: {pasta_data}")

def carregar_dados(nome_arquivo):
    caminho_completo = os.path.join(pasta_data, nome_arquivo)
    
    if not os.path.exists(caminho_completo):
        raise FileNotFoundError(f"❌ Erro crítico: O arquivo {nome_arquivo} não existe em {pasta_data}")
    
    return pd.read_csv(caminho_completo, sep=';', encoding='utf-8-sig')






class DatabaseManager:
    def __init__(self, db_path=None):
        # Descobre onde este arquivo (database_manager.py) está
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
    # Sobe um nível para achar a pasta 'data' que está na raiz
        self.project_root = os.path.dirname(self.base_dir)
    
        if db_path is None:
        # Define o caminho para o banco dentro de 'data'
            self.db_path = os.path.join(self.project_root, 'data', 'aanc_gestamp.db')
        else:
            self.db_path = db_path
        
        self.conn = duckdb.connect(database=self.db_path)
        self.pasta_data = os.path.join(self.project_root, 'data')
    
    # Inicializa as tabelas usando a lógica do Pandas que limpamos antes
        self.inicializar_tabelas_limpas()
        

    def inicializar_tabelas_limpas(self):
        """Versão Ultra-Resistente: Resolve o erro de benchmark e garante os dados"""
        arquivos = {
            'benchmark_mercado.csv': 'benchmark',
            'mapeamento_sinonimos.csv': 'sinonimos',
            'headcount.csv': 'headcount',
            'premissas_plantas.csv': 'premissas'
        }

        for arquivo, tabela in arquivos.items():
            caminho_csv = os.path.join(self.pasta_data, arquivo)
            
            if os.path.exists(caminho_csv):
                try:
                    # Se for o benchmark, usamos o separador ';' que vimos no erro
                    if tabela == 'benchmark':
                        df = pd.read_csv(caminho_csv, sep=';', encoding='utf-8-sig', on_bad_lines='skip')
                    else:
                        # Para os outros, deixamos o Pandas tentar adivinhar
                        df = pd.read_csv(caminho_csv, sep=None, engine='python', encoding='utf-8-sig')
                    
                    # Limpeza total dos nomes das colunas
                    df.columns = [str(c).replace('"', '').strip() for c in df.columns]
                    
                    # Garante que a primeira coluna do benchmark se chame 'Filial'
                    if tabela == 'benchmark':
                        df.rename(columns={df.columns[0]: 'Filial'}, inplace=True)
                        # Limpa espaços dentro da coluna Filial (ex: 'G1 ' vira 'G1')
                        df['Filial'] = df['Filial'].astype(str).str.strip()

                    self.conn.register('df_temp', df)
                    self.conn.execute(f"CREATE OR REPLACE TABLE {tabela} AS SELECT * FROM df_temp")
                    self.conn.unregister('df_temp')
                    print(f"✅ {tabela} carregada e pronta para o combate!")
                    
                except Exception as e:
                    print(f"❌ Erro fatal ao carregar {arquivo}: {e}")

    def fechar_conexao(self):
        self.con.close()

    def get_mapa_sinonimos(self):
        """
        Lê o arquivo de mapeamento de sinônimos e retorna um dicionário de sinônimos para planta.

        O CSV esperado é `data/mapeamento_sinonimos.csv` com separador `;`.
        Cada linha contém um ID de planta e seus sinônimos separados por vírgula.

        Returns:
            dict: Dicionário onde cada sinônimo em minúsculas é chave e o ID da planta é valor.

        Raises:
            Exception: Se o arquivo de mapeamento não for encontrado ou não puder ser lido.
        """
        try:
            df = pd.read_csv('data/mapeamento_sinonimos.csv', sep=';', quotechar='"')
            mapa = {}

            for _, row in df.iterrows():
                planta_id = str(row.get('Planta') or row.get('planta') or row.get('ID') or row.get('id')).strip()
                sinonimos = str(row.get('Sinonimos') or row.get('sinônimos') or row.get('Sinônimos') or '').strip()
                for sin in [s.strip().lower() for s in sinonimos.split(',') if s.strip()]:
                    mapa[sin] = planta_id

            return mapa
        except FileNotFoundError as e:
            raise Exception(f"Arquivo de mapeamento de sinônimos não encontrado: {str(e)}")
        except Exception as e:
            raise Exception(f"Erro ao ler o mapeamento de sinônimos: {str(e)}")

    def executar_consulta(self, sql_query):
        """
        Executa uma consulta SQL e retorna os resultados em um DataFrame do Pandas.

        Args:
            sql_query (str): A consulta SQL a ser executada.

        Returns:
            pd.DataFrame: DataFrame contendo os resultados da consulta.

        Raises:
            Exception: Se a consulta SQL falhar, retorna uma mensagem de erro orientativa.
        """
        try:
            result = self.conn.execute(sql_query)
            df = result.fetchdf()
            return df
        except Exception as e:
            error_msg = str(e)
            if "no such table" in error_msg.lower():
                raise Exception("Erro: A tabela especificada na consulta não existe. Verifique se as tabelas foram inicializadas corretamente.")
            elif "syntax error" in error_msg.lower() or "parse error" in error_msg.lower():
                raise Exception("Erro: A consulta SQL contém um erro de sintaxe. Verifique a estrutura da consulta e tente novamente.")
            else:
                raise Exception(f"Erro ao executar consulta SQL: {error_msg}. Verifique a consulta e tente novamente.")

    def simular_cenario_completo(self, planta_id, pct_salario, pct_va, pct_plr, pct_he_adicional):
        """
        Simula um cenário de custo completo para uma planta específica usando fórmula de custo cascata.

        Fórmula: Custo = ((Salário + (Perc_HE * Salário * 1.2)) * (1 + Prov_Férias + Prov_13)) * (1 + RAT + FGTS + INSS + Terceiros)

        Args:
            planta_id (str): Identificador da planta (ex: 'G1').
            pct_salario (float): Percentual de aumento aplicado ao salário base.
            pct_va (float): Percentual de aumento aplicado ao VA.
            pct_plr (float): Percentual de aumento aplicado ao PLR.
            pct_he_adicional (float): Percentual de aumento aplicado às horas extras.

        Returns:
            dict: Dicionário com os principais indicadores de custo.
        """
        try:
            # Buscar todos os colaboradores da planta com suas premissas
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

            # Validação de dados nulos
            df = df.fillna(0)

            # Inicializar variáveis de acumulação
            custo_atual_total = 0.0
            custo_projetado_total = 0.0
            total_va_atual = 0.0
            total_plr_atual = 0.0
            novo_va_total = 0.0
            novo_plr_total = 0.0
            total_salario_atual_encargos = 0.0
            total_salario_projetado_encargos = 0.0

            # Iterar por cada colaborador para calcular custos
            for idx, row in df.iterrows():
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

                # Cálculo ATUAL: Fórmula cascata APENAS para salários
                # Salários: ((Salário + (Perc_HE * Salário * 1.2)) * (1 + Prov_Férias + Prov_13)) * (1 + Encargos)
                base_atual = salario_atual + (perc_he_medio * salario_atual * 1.2)
                com_provisoes_atual = base_atual * (1 + provisao_ferias + provisao_13)
                custo_salario_atual = com_provisoes_atual * (1 + rat_fap + fgts + inss_patronal + terceiros)

                # VA e PLR: Apenas valores base (sem encargos/provisões)
                custo_va_atual = va_atual
                custo_plr_atual = plr_atual

                # Acumular custo atual
                custo_atual_total += custo_salario_atual + custo_va_atual + custo_plr_atual

                # Cálculo PROJETADO: Aplicar reajustes antes da fórmula cascata
                salario_novo = salario_atual * (1 + pct_salario / 100)
                va_novo = va_atual * (1 + pct_va / 100)
                plr_novo = plr_atual * (1 + pct_plr / 100)
                perc_he_novo = perc_he_medio * (1 + pct_he_adicional / 100)

                # Salários: Aplicar fórmula cascata
                base_projetado = salario_novo + (perc_he_novo * salario_novo * 1.2)
                com_provisoes_projetado = base_projetado * (1 + provisao_ferias + provisao_13)
                custo_salario_projetado = com_provisoes_projetado * (1 + rat_fap + fgts + inss_patronal + terceiros)

                # VA e PLR: Apenas valores reajustados (sem encargos/provisões)
                custo_va_projetado = va_novo
                custo_plr_projetado = plr_novo

                # Acumular custo projetado
                custo_projetado_total += custo_salario_projetado + custo_va_projetado + custo_plr_projetado

                # Acumular totais para detalhes
                total_va_atual += va_atual
                total_plr_atual += plr_atual
                novo_va_total += va_novo
                novo_plr_total += plr_novo
                total_salario_atual_encargos += custo_salario_atual
                total_salario_projetado_encargos += custo_salario_projetado

            # Cálculo de impacto ANUAL (multiplicar custos mensais por 12, PLR já é anual)
            salario_atual_anual = total_salario_atual_encargos * 12
            salario_projetado_anual = total_salario_projetado_encargos * 12
            va_atual_anual = total_va_atual * 12
            va_projetado_anual = novo_va_total * 12
            plr_atual_anual = total_plr_atual
            plr_projetado_anual = novo_plr_total

            custo_atual_anual = salario_atual_anual + va_atual_anual + plr_atual_anual
            custo_projetado_anual = salario_projetado_anual + va_projetado_anual + plr_projetado_anual
            impacto_anual = custo_projetado_anual - custo_atual_anual
            
            # Detalhes: Somas dos componentes base (sem encargos/provisões)
            soma_salario_atual = df['salario_atual'].sum()
            soma_salario_novo = soma_salario_atual * (1 + pct_salario / 100)
            soma_he_atual = (df['salario_atual'] * df['perc_he_medio'] * 1.2).sum()
            soma_he_novo = (df['salario_atual'] * (1 + pct_salario / 100) * (df['perc_he_medio'] * (1 + pct_he_adicional / 100)) * 1.2).sum()

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
                    'Custo PLR Projetado Anual': round(plr_projetado_anual, 2),
                    'Salário Base Atual': round(soma_salario_atual, 2),
                    'HE Atual': round(soma_he_atual, 2),
                    'DSR Atual': 0.0,
                    'VA Atual': round(total_va_atual, 2),
                    'PLR Atual': round(total_plr_atual, 2),
                    'Novo Salário': round(soma_salario_novo, 2),
                    'Novo HE': round(soma_he_novo, 2),
                    'DSR Projetado': 0.0,
                    'Novo VA': round(novo_va_total, 2),
                    'Novo PLR': round(novo_plr_total, 2)
                }
            }
        except Exception as e:
            raise Exception(f"Erro ao simular cenário para a planta {planta_id}: {str(e)}")

    def obter_benchmark(self, planta_id):
        try:
            # O UPPER garante que ele ache 'G1' mesmo que venha 'g1' do site
            query = f"SELECT * FROM benchmark WHERE UPPER(Filial) = UPPER('{planta_id}')"
            df_filtrado = self.conn.execute(query).df()

            if df_filtrado.empty:
                return {'message': f'Sem dados de benchmark para {planta_id}.'}
            return df_filtrado
        except Exception as e:
            return {'message': f"Erro no banco: {str(e)}"}
    
    def obter_documentos_por_planta(self, planta_id):
        """
        Obtém a lista de documentos de referência associados a uma planta específica.

        Args:
            planta_id (str): O ID da planta (ex: 'G1').

        Returns:
            list: Lista de nomes de arquivos de documentos associados à planta.

        Raises:
            Exception: Se a consulta falhar.
        """
        try:
            query = f"SELECT \"Documento Referência\" FROM referencias_sindicais WHERE Filial = '{planta_id}'"
            df = self.executar_consulta(query)
            return df["Documento Referência"].tolist()
        except Exception as e:
            raise Exception(f"Erro ao obter documentos para a planta {planta_id}: {str(e)}")

if __name__ == '__main__':
    # 1. Instanciar a classe
    dm = DatabaseManager()

    # 2. Inicializar as tabelas (isso limpa os CSVs e cria o banco .db)
    dm.inicializar_tabelas_limpas()

    # --- TESTE 1: Headcount e Premissas (O que você já tinha) ---
    query_folha = """
    SELECT COUNT(*) as total_colaboradores, AVG(h.salario_atual) as media_salarial
    FROM headcount h
    JOIN premissas p ON h.planta = p.planta
    WHERE p.planta = 'G1'
    """
    try:
        resultado_folha = dm.executar_consulta(query_folha)
        print("\n📊 Resultado da Folha (G1):")
        print(resultado_folha)
    except Exception as e:
        print(f"❌ Erro no teste de folha: {e}")

    # --- TESTE 2: Benchmark (O que está falhando no site) ---
    print("\n🔍 Testando busca de Mercado (Benchmark) para G1:")
    try:
        resultado_mercado = dm.obter_benchmark('G1')
        print(resultado_mercado)
    except Exception as e:
        print(f"❌ Erro no teste de benchmark: {e}")