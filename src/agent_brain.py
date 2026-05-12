"""Módulo que orquestra o agente de negociação integrando SQL determinístico, RAG e geração de linguagem."""

import os
from dotenv import load_dotenv
try:
    from database_manager import DatabaseManager
    from rag_engine import DocumentAssistant
except ImportError:
    from src.database_manager import DatabaseManager
    from src.rag_engine import DocumentAssistant

from google import genai

load_dotenv()

class AANC_Agent:
    """
    Classe orquestradora que integra o DatabaseManager (SQL) e DocumentAssistant (RAG).
    Utiliza Gemini para classificar a intenção da pergunta e rotear para o módulo apropriado.
    """

    def __init__(self):
        """
        Inicializa o agente com DatabaseManager, DocumentAssistant e Gemini.
        """
        try:
            # Inicializar DatabaseManager
            self.dm = DatabaseManager()
            self.dm.inicializar_tabelas_limpas()
            print("DatabaseManager inicializado com sucesso.")

            # Inicializar DocumentAssistant
            self.da = DocumentAssistant()
            try:
                self.da.indexar_documentos('docs/')
                print("DocumentAssistant indexado com sucesso.")
            except Exception as e:
                print(f"Aviso ao indexar documentos: {e}")

            # Inicializar Gemini
            api_key = os.getenv('GOOGLE_API_KEY')
            if not api_key:
                raise Exception("Chave GOOGLE_API_KEY não encontrada no arquivo .env ou nas variáveis de ambiente.")
            self.client = genai.Client(api_key=api_key)
            print("Gemini inicializado com sucesso usando gemini-flash-latest.")

        except Exception as e:
            raise Exception(f"Erro ao inicializar AANC_Agent: {str(e)}")

    def _classificar_intencao(self, pergunta):
        """
        Classifica a intenção da pergunta usando Gemini: 'CÁLCULO', 'POLÍTICA', 'CÁLCULO_FINANCEIRO', 'BENCHMARK' ou 'CONSULTA_ESTRATEGICA'.

        Args:
            pergunta (str): A pergunta do usuário.

        Returns:
            str: 'CÁLCULO', 'POLÍTICA', 'CÁLCULO_FINANCEIRO', 'BENCHMARK', 'CONSULTA_ESTRATEGICA' ou None se não for possível classificar.
        """
        prompt = f"""Classifique a seguinte pergunta como 'CÁLCULO', 'POLÍTICA', 'CÁLCULO_FINANCEIRO', 'BENCHMARK' ou 'CONSULTA_ESTRATEGICA':

- CÁLCULO: perguntas sobre salários, PLR, impacto financeiro, cálculos de benefícios, etc. (consultas gerais de dados)
- POLÍTICA: perguntas sobre cláusulas, regras, procedimentos, textos de documentos, jornadas, políticas.
- CÁLCULO_FINANCEIRO: perguntas sobre impacto, reajuste, aumento, simulação de custos, cenários financeiros, percentuais (%).
- BENCHMARK: perguntas sobre concorrência, mercado, práticas de outras empresas (Fiat, Renault, Hyundai, VW, etc.), comparações com o mercado.
- CONSULTA_ESTRATEGICA: perguntas genéricas sobre reajuste, 'como está', mercado ou benchmark de uma empresa/planta, onde o agente deve comparar benchmark de mercado com o ACT/CCT da planta.

Pergunta: "{pergunta}"

Responda apenas com uma das palavras: 'CÁLCULO', 'POLÍTICA', 'CÁLCULO_FINANCEIRO', 'BENCHMARK' ou 'CONSULTA_ESTRATEGICA'."""

        try:
            response = self.client.models.generate_content(model='gemini-flash-latest', contents=prompt)
            intencao = response.text.strip().upper()
            if intencao in ['CÁLCULO', 'POLÍTICA', 'CÁLCULO_FINANCEIRO', 'BENCHMARK', 'CONSULTA_ESTRATEGICA']:
                return intencao
            return None
        except Exception as e:
            print(f"Erro ao classificar intenção: {e}")
            return None

    def _extrair_variaveis_simulacao(self, pergunta):
        """
        Extrai variáveis de simulação financeira da pergunta usando Gemini.

        Args:
            pergunta (str): A pergunta do usuário.

        Returns:
            dict: Dicionário com reajuste_salarial, reajuste_va, reajuste_plr, aumento_he (todos floats, padrão 0).
        """
        prompt = f"""Analise a seguinte pergunta e extraia os valores percentuais mencionados para simulação financeira.

Pergunta: "{pergunta}"

Extraia os seguintes valores (em percentual, sem o símbolo %):
- reajuste_salarial: percentual de reajuste salarial mencionado
- reajuste_va: percentual de reajuste do VA mencionado
- reajuste_plr: percentual de reajuste do PLR mencionado
- aumento_he: percentual de aumento das horas extras mencionado

Se um valor não for mencionado, use 0.

Responda apenas com um JSON válido no formato:
{{"reajuste_salarial": 0, "reajuste_va": 0, "reajuste_plr": 0, "aumento_he": 0}}"""

        try:
            response = self.client.models.generate_content(model='gemini-flash-latest', contents=prompt)
            texto = response.text.strip()
            # Tentar parsear JSON
            import json
            dados = json.loads(texto)
            return {
                'pct_salario': float(dados.get('reajuste_salarial', 0)),
                'pct_va': float(dados.get('reajuste_va', 0)),
                'pct_plr': float(dados.get('reajuste_plr', 0)),
                'pct_he_adicional': float(dados.get('aumento_he', 0))
            }
        except Exception as e:
            print(f"Erro ao extrair variáveis: {e}. Usando valores padrão.")
            return {'pct_salario': 0, 'pct_va': 0, 'pct_plr': 0, 'pct_he_adicional': 0}

    def processar_pergunta(self, pergunta, planta_id):
        """
        Processa a pergunta do usuário, classifica a intenção e roteia para o módulo apropriado.

        Args:
            pergunta (str): A pergunta do usuário.
            planta_id (str): O ID da planta (ex: 'G1').

        Returns:
            dict: Resposta estruturada com 'tipo', 'contexto', 'resposta'.

        Raises:
            Exception: Se a pergunta não puder ser processada.
        """
        try:
            # Obter documentos permitidos para a planta
            arquivos_permitidos = self.dm.obter_documentos_por_planta(planta_id)
            if not arquivos_permitidos:
                return {
                    "tipo": "ERRO",
                    "contexto": f"Planta {planta_id}",
                    "resposta": f"Nenhum documento encontrado para a planta {planta_id}. Verifique se a planta existe no sistema."
                }

            # Classificar intenção
            intencao = self._classificar_intencao(pergunta)
            if not intencao:
                if self._detectar_consulta_estrategica(pergunta):
                    intencao = 'CONSULTA_ESTRATEGICA'
                else:
                    intencao = 'POLÍTICA'

            if intencao == 'CÁLCULO':
                return self._processar_calculo(pergunta, planta_id)
            elif intencao == 'CÁLCULO_FINANCEIRO':
                return self._processar_simulacao_financeira(pergunta, planta_id)
            elif intencao == 'BENCHMARK':
                return self._processar_benchmark(pergunta, planta_id)
            elif intencao == 'CONSULTA_ESTRATEGICA':
                return self._processar_consulta_estrategica(pergunta, planta_id, arquivos_permitidos)
            return self._processar_politica(pergunta, planta_id, arquivos_permitidos)

        except Exception as e:
            return {
                "tipo": "ERRO",
                "contexto": planta_id,
                "resposta": f"Desculpe, não consegui processar sua pergunta. Por favor, reformule a pergunta e tente novamente. Erro: {str(e)}"
            }

    def _detectar_consulta_estrategica(self, pergunta):
        """
        Detecta perguntas genéricas de consulta estratégica sobre reajuste, mercado ou benchmark.
        """
        texto = pergunta.lower()
        palavras_chave = [
            'reajuste',
            'como está',
            'como esta',
            'mercado',
            'benchmark',
            'praticado',
            'acordo coletivo',
            'act',
            'cct'
        ]
        return any(p in texto for p in palavras_chave)

    def _processar_consulta_estrategica(self, pergunta, planta_id, arquivos_permitidos):
        """
        Processa perguntas genéricas de consulta estratégica usando benchmark e contexto ACT/CCT.

        Args:
            pergunta (str): A pergunta do usuário.
            planta_id (str): O ID da planta.
            arquivos_permitidos (list): Lista de arquivos permitidos.

        Returns:
            dict: Resposta estruturada.
        """
        try:
            benchmark_df = self.dm.obter_benchmark(planta_id)
            if isinstance(benchmark_df, dict):
                return {
                    "tipo": "CONSULTA_ESTRATEGICA",
                    "contexto": planta_id,
                    "resposta": benchmark_df.get('message', 'Dados de benchmark não cadastrados para esta unidade.'),
                    "documentos_consultados": [],
                    "benchmark_dados": []
                }

            contextos = self.da.buscar_contexto_especifico(pergunta, arquivos_permitidos)
            contexto_texto = "\n---\n".join([c["text"] for c in contextos])
            arquivos_consultados = [c["file"] for c in contextos]

            prompt = f"""Você é um assistente consultivo que responde perguntas sobre prática de mercado e documentos trabalhistas.

Para a planta {planta_id}, o mercado (Benchmark) praticou os valores abaixo. Já o nosso documento ACT/CCT da planta prevê os valores e as regras descritas no contexto. Não diga que a pergunta é ambígua.

PERGUNTA: "{pergunta}"

BENCHMARK:
{benchmark_df.to_string(index=False)}

CONTEXTOS DO ACT/CCT:
{contexto_texto}

INSTRUÇÕES:
1. Informe claramente o percentual de mercado e indique que o documento ACT/CCT traz a previsão da planta.
2. Faça uma comparação consultiva entre benchmark e documento.
3. Seja objetivo e evite respostas vagas.
4. Inclua um breve resumo das diferenças ou similaridades.

Responda em português, usando a frase: "Para a [Empresa/Planta], o mercado (Benchmark) praticou X%. Já o nosso documento (ACT/CCT) prevê Y.""" 

            response = self.client.models.generate_content(model='gemini-flash-latest', contents=prompt)

            return {
                "tipo": "CONSULTA_ESTRATEGICA",
                "contexto": planta_id,
                "benchmark_dados": benchmark_df.to_dict('records'),
                "documentos_consultados": arquivos_consultados,
                "resposta": response.text.strip()
            }
        except Exception as e:
            return {
                "tipo": "CONSULTA_ESTRATEGICA",
                "contexto": planta_id,
                "resposta": f"Não consegui processar a consulta estratégica. Verifique se há dados de benchmark e documentos ACT/CCT disponíveis para a planta {planta_id}. Erro: {str(e)}"
            }

    def _processar_calculo(self, pergunta, planta_id):
        """
        Processa perguntas de cálculo usando SQL.

        Args:
            pergunta (str): A pergunta de cálculo.
            planta_id (str): O ID da planta.

        Returns:
            dict: Resposta estruturada.
        """
        try:
            # Usar Gemini para gerar query SQL
            prompt = f"""Gere uma consulta SQL DuckDB para responder a seguinte pergunta sobre a planta {planta_id}:

"{pergunta}"

Use as tabelas disponíveis:
- headcount: com colunas id_funcionario, planta, subgrupo_cargos, salario_atual, valor_va_atual, ajuda_combustivel_atual, plr_elegivel, plr_alvo_atual
- premissas: com colunas planta, ajuda_combustivel_planta, plr_alvo_planta, rat_fap, perc_he_medio, fgts, provisao_ferias, provisao_13, inss_patronal, terceiros

Retorne apenas a query SQL, sem explicações."""

            response = self.client.models.generate_content(model='gemini-flash-latest', contents=prompt)
            sql_query = response.text.strip()

            # Executar query
            resultado = self.dm.executar_consulta(sql_query)

            # Formatar resposta
            return {
                "tipo": "CÁLCULO",
                "contexto": planta_id,
                "query_sql": sql_query,
                "resultado": resultado.to_string(),
                "resposta": f"Consulta executada com sucesso:\n\n{resultado.to_string()}"
            }
        except Exception as e:
            return {
                "tipo": "CÁLCULO",
                "contexto": planta_id,
                "resposta": f"Não consegui processar a pergunta de cálculo. Reformule a pergunta com termos relacionados a salário, PLR ou benefícios. Erro: {str(e)}"
            }

    def _processar_simulacao_financeira(self, pergunta, planta_id):
        """
        Processa simulações financeiras usando o método simular_cenario_completo.

        Args:
            pergunta (str): A pergunta de simulação financeira.
            planta_id (str): O ID da planta.

        Returns:
            dict: Resposta estruturada.
        """
        try:
            # Extrair variáveis da pergunta
            variaveis = self._extrair_variaveis_simulacao(pergunta)

            # Executar simulação
            resultado_simulacao = self.dm.simular_cenario_completo(
                planta_id=planta_id,
                pct_salario=variaveis['pct_salario'],
                pct_va=variaveis['pct_va'],
                pct_plr=variaveis['pct_plr'],
                pct_he_adicional=variaveis['pct_he_adicional']
            )

            # Gerar explicação com Gemini
            prompt_explicacao = f"""Explique o resultado da simulação financeira para a planta {planta_id} de forma clara e concisa.

Resultado da Simulação:
{resultado_simulacao}

Destaque que o cálculo incluiu:
- Encargos sociais (INSS, FGTS, RAT, Terceiros)
- Provisões (Férias e 13º salário)
- Reflexos de HE/DSR conforme as premissas específicas da planta

Responda em português, focando nos impactos principais."""

            response = self.client.models.generate_content(model='gemini-flash-latest', contents=prompt_explicacao)
            explicacao = response.text.strip()

            return {
                "tipo": "CÁLCULO_FINANCEIRO",
                "contexto": planta_id,
                "variaveis_extraidas": variaveis,
                "resultado_simulacao": resultado_simulacao,
                "resposta": explicacao
            }
        except Exception as e:
            return {
                "tipo": "CÁLCULO_FINANCEIRO",
                "contexto": planta_id,
                "resposta": f"Não consegui processar a simulação financeira. Verifique se os percentuais estão corretos. Erro: {str(e)}"
            }

    def _processar_benchmark(self, pergunta, planta_id):
        """
        Processa perguntas sobre benchmark de mercado, comparando nossa prática com a concorrência.

        Args:
            pergunta (str): A pergunta sobre benchmark/concorrência.
            planta_id (str): O ID da planta.

        Returns:
            dict: Resposta estruturada com comparação consultiva.
        """
        try:
            # Obter dados de benchmark da concorrência
            benchmark_df = self.dm.obter_benchmark(planta_id)
            if isinstance(benchmark_df, dict):
                return {
                    "tipo": "BENCHMARK",
                    "contexto": planta_id,
                    "resposta": benchmark_df.get('message', 'Dados de benchmark não cadastrados para esta unidade.'),
                    "benchmark_dados": []
                }

            # Obter dados atuais da nossa prática (médias por planta)
            query_nossa_pratica = f"""
                SELECT
                    AVG(salario_atual) as salario_medio,
                    AVG(valor_va_atual) as va_medio,
                    AVG(plr_alvo_atual) as plr_medio,
                    COUNT(*) as num_colaboradores
                FROM headcount
                WHERE planta = '{planta_id}'
            """
            nossa_pratica_df = self.dm.executar_consulta(query_nossa_pratica)

            # Preparar dados para o Gemini
            benchmark_texto = benchmark_df.to_string(index=False)
            nossa_pratica_texto = nossa_pratica_df.to_string(index=False)

            # Instruir Gemini a comparar e dar resposta consultiva
            prompt = f"""Analise a seguinte pergunta sobre benchmark de mercado e forneça uma resposta consultiva comparando nossa prática com a concorrência.

PERGUNTA: "{pergunta}"

NOSSA PRÁTICA ATUAL (dados da planta {planta_id}):
{nossa_pratica_texto}

PRÁTICA DO MERCADO (benchmark da concorrência):
{benchmark_texto}

INSTRUÇÕES PARA RESPOSTA:
1. Compare sempre: "Nossa Prática (SQL)" vs "Prática do Mercado (Benchmark)"
2. Seja consultivo: indique se estamos acima/abaixo/na média do mercado
3. Destaque pontos fortes e oportunidades de melhoria
4. Considere fatores como região, porte da empresa e tendências do setor
5. Sugira ações práticas baseadas na comparação

Responda em português de forma clara e objetiva."""

            response = self.client.models.generate_content(model='gemini-flash-latest', contents=prompt)

            return {
                "tipo": "BENCHMARK",
                "contexto": planta_id,
                "benchmark_dados": benchmark_df.to_dict('records'),
                "nossa_pratica": nossa_pratica_df.to_dict('records'),
                "resposta": response.text.strip()
            }
        except Exception as e:
            return {
                "tipo": "BENCHMARK",
                "contexto": planta_id,
                "resposta": f"Não consegui processar a análise de benchmark. Verifique se há dados de concorrência disponíveis para a planta {planta_id}. Erro: {str(e)}"
            }

    def _processar_politica(self, pergunta, planta_id, arquivos_permitidos):
        """
        Processa perguntas de política usando RAG.

        Args:
            pergunta (str): A pergunta de política.
            planta_id (str): O ID da planta.
            arquivos_permitidos (list): Lista de arquivos permitidos.

        Returns:
            dict: Resposta estruturada.
        """
        try:
            # Buscar contexto nos documentos
            contextos = self.da.buscar_contexto_especifico(pergunta, arquivos_permitidos)

            # Usar Gemini para gerar resposta
            contexto_texto = "\n---\n".join([c["text"] for c in contextos])
            prompt = f"""Baseado no seguinte contexto dos documentos da planta {planta_id}, responda à pergunta do usuário:

CONTEXTO:
{contexto_texto}

PERGUNTA: "{pergunta}"

Responda de forma clara e concisa em português, citando as fontes quando relevante."""

            response = self.client.models.generate_content(model='gemini-flash-latest', contents=prompt)

            return {
                "tipo": "POLÍTICA",
                "contexto": planta_id,
                "documentos_consultados": [c["file"] for c in contextos],
                "trechos": [c["text"] for c in contextos],
                "resposta": response.text.strip()
            }
        except Exception as e:
            return {
                "tipo": "POLÍTICA",
                "contexto": planta_id,
                "resposta": f"Desculpe, não encontrei informações nos documentos para responder sua pergunta. Tente reformular a pergunta ou verifique se os documentos estão disponíveis. Erro: {str(e)}"
            }

if __name__ == '__main__':
    try:
        # Instanciar agente
        agent = AANC_Agent()
        print("=" * 80)
        print("AANC_Agent inicializado com sucesso!")
        print("=" * 80)

        # Testar pergunta para G1
        planta_teste = 'G1'
        pergunta_teste = 'Quais as regras de jornada de trabalho para Curitiba?'

        print(f"\nPlanta: {planta_teste}")
        print(f"Pergunta: {pergunta_teste}")
        print("-" * 80)

        resultado = agent.processar_pergunta(pergunta_teste, planta_teste)

        print(f"Tipo de Resposta: {resultado.get('tipo')}")
        print(f"Contexto: {resultado.get('contexto')}")
        if 'documentos_consultados' in resultado:
            print(f"Documentos Consultados: {resultado.get('documentos_consultados')}")
        if 'trechos' in resultado:
            print("\nTrechos retornados pelo RAG:")
            for i, trecho in enumerate(resultado.get('trechos', []), start=1):
                print(f"{i}. {trecho[:300].replace('\n', ' ')}...")
        print(f"\nResposta:\n{resultado.get('resposta')}")
        print("\nSUCESSO: O Maestro está online e operante!")
    except Exception as e:
        print(f"Erro ao executar teste: {e}")