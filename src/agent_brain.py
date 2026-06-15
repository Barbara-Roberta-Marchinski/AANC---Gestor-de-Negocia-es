"""Módulo que orquestra o agente de negociação integrando SQL determinístico, RAG e geração de linguagem."""

import os
import joblib
import pandas as pd
from dotenv import load_dotenv
try:
    from database_manager import DataManager
    from rag_engine import DocumentAssistant
except ImportError:
    from src.database_manager import DataManager
    from src.rag_engine import DocumentAssistant

from google import genai

load_dotenv()

class AANC_Agent:
    """
    Classe orquestradora que integra o DataManager (SQL) e DocumentAssistant (RAG).
    Utiliza Gemini para classificar a intenção da pergunta e rotear para o módulo apropriado.
    """

    def __init__(self):
        """
        Inicializa o agente com DataManager, DocumentAssistant e Gemini.
        """
        try:
            # Inicializar DataManager
            self.dm = DataManager()
            self.dm.inicializar_tabelas()
            print("DataManager inicializado com sucesso.")

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

    def _identificar_tarefas(self, pergunta):
        """
        Identifica todas as tarefas necessárias para atender à pergunta do usuário.

        Retorna uma lista de tarefas que devem ser executadas sequencialmente.
        """
        prompt = f"""Você é um orquestrador de ferramentas responsável por analisar o pedido do usuário e selecionar todas as ações necessárias.

Ações possíveis:
- CÁLCULO: gerar e executar consulta SQL para cálculos de salário, PLR, VA, encargos e impacto financeiro.
- CÁLCULO_FINANCEIRO: executar simulação financeira de reajustes e explicar o impacto na folha de pagamento.
- RISCO_ML: calcular o risco de greve, evasão ou turnover usando o modelo ML.
- BENCHMARK: comparar nossa prática com benchmark de mercado.
- POLÍTICA: responder com base nos documentos ACT/CCT usando RAG.
- CONSULTA_ESTRATEGICA: responder consultas estratégicas que misturam mercado, política e reajuste.

Instruções:
1. Analise o pedido do usuário com atenção e não omita nenhuma frente de análise relevante.
2. Se o pedido envolver tanto custo financeiro quanto risco de greve/turnover, inclua CÁLCULO_FINANCEIRO e RISCO_ML.
3. Se houver pedido de regras e documentos, adicione POLÍTICA.
4. Se houver pedido de comparação de mercado, adicione BENCHMARK.
5. REGRA DE MÚLTIPLAS FERRAMENTAS: Se você acionar mais de uma ferramenta para o mesmo cenário, você é OBRIGADO a repassar exatamente os mesmos parâmetros (como ID da Planta e % de reajuste) para TODAS as funções. Não deixe parâmetros vazios se a informação estiver na pergunta do usuário.
6. Retorne apenas JSON válido no formato:
{{"tasks": ["CÁLCULO_FINANCEIRO", "RISCO_ML"]}}

Usuário: "{pergunta}"
"""
        try:
            response = self.client.models.generate_content(model='gemini-flash-latest', contents=prompt)
            texto = response.text.strip()
            import json, re, ast

            # Extrair JSON do texto, se existir
            match = re.search(r"\{.*\}", texto, re.S)
            dados = None
            if match:
                texto_json = match.group()
                try:
                    dados = json.loads(texto_json)
                except json.JSONDecodeError:
                    dados = ast.literal_eval(texto_json)

            if dados and isinstance(dados, dict):
                tarefas = dados.get('tasks') or dados.get('task') or dados.get('actions')
                if isinstance(tarefas, str):
                    tarefas = [tarefas]
                if isinstance(tarefas, (list, tuple)) and tarefas:
                    return [str(t).strip().upper() for t in tarefas if str(t).strip()]
        except Exception as e:
            print(f"Erro ao identificar tarefas: {e}")

        # Fallback heurístico simples
        tarefas = []
        if self._detectar_risco_ml(pergunta):
            tarefas.append('RISCO_ML')
        if any(word in pergunta.lower() for word in ['reajuste', '%', 'salário', 'folha', 'impacto']):
            tarefas.append('CÁLCULO_FINANCEIRO')
        if any(word in pergunta.lower() for word in ['act', 'cct', 'acordo coletivo', 'cláusula', 'contrato']):
            tarefas.append('POLÍTICA')
        if any(word in pergunta.lower() for word in ['benchmark', 'mercado', 'concorrência', 'prática de mercado']):
            tarefas.append('BENCHMARK')
        if not tarefas:
            intencao = self._classificar_intencao(pergunta)
            if intencao:
                tarefas.append(intencao)
            elif self._detectar_consulta_estrategica(pergunta):
                tarefas.append('CONSULTA_ESTRATEGICA')
            else:
                tarefas.append('POLÍTICA')
        return list(dict.fromkeys(tarefas))

    def _processar_varias_tarefas(self, tarefas, pergunta, planta_id, arquivos_permitidos):
        """Executa várias tarefas em sequência quando a pergunta exige mais de uma análise."""
        variaveis_compartilhadas = self._extrair_variaveis_simulacao(pergunta)
        componentes = []
        for tarefa in tarefas:
            try:
                if tarefa == 'CÁLCULO':
                    componentes.append(self._processar_calculo(pergunta, planta_id))
                elif tarefa == 'CÁLCULO_FINANCEIRO':
                    componentes.append(self._processar_simulacao_financeira(pergunta, planta_id, variaveis=variaveis_compartilhadas))
                elif tarefa == 'RISCO_ML':
                    componentes.append(self._processar_risco_evasao_ml(pergunta, planta_id, variaveis=variaveis_compartilhadas))
                elif tarefa == 'BENCHMARK':
                    componentes.append(self._processar_benchmark(pergunta, planta_id))
                elif tarefa == 'CONSULTA_ESTRATEGICA':
                    componentes.append(self._processar_consulta_estrategica(pergunta, planta_id, arquivos_permitidos))
                else:
                    componentes.append(self._processar_politica(pergunta, planta_id, arquivos_permitidos))
            except Exception:
                if tarefa == 'POLÍTICA':
                    componentes.append({
                        'tipo': 'POLÍTICA',
                        'contexto': planta_id,
                        'documentos_consultados': [],
                        'trechos': [],
                        'resposta': 'Nenhuma política limitante aplicável encontrada.'
                    })
                else:
                    componentes.append({
                        'tipo': tarefa,
                        'contexto': planta_id,
                        'resposta': 'Não foi possível concluir esta etapa. Tente novamente ou reformule a pergunta.'
                    })

        texto_resposta = '\n\n'.join([
            f"[{comp.get('tipo', 'N/A')}] {comp.get('resposta', '')}" for comp in componentes
        ])

        return {
            'tipo': 'MULTIPLO',
            'contexto': planta_id,
            'tarefas': tarefas,
            'componentes': componentes,
            'resposta': texto_resposta,
        }

    def _extrair_variaveis_simulacao(self, pergunta):
        """
        Extrai variáveis de simulação financeira da pergunta usando Gemini.

        Args:
            pergunta (str): A pergunta do usuário.

        Returns:
            dict: Dicionário com reajuste_salarial, reajuste_va, reajuste_plr, aumento_he (todos floats, padrão 0).
        """
        import re

        valores = {
            'pct_salario': 0.0,
            'pct_va': 0.0,
            'pct_plr': 0.0,
            'pct_he_adicional': 0.0
        }

        # Extrai todos os percentuais explícitos da pergunta.
        padrao = re.compile(r"(\d+(?:[\.,]\d+)?)\s*(%|por cento|porcento)", re.IGNORECASE)
        for match in padrao.finditer(pergunta):
            numero = float(match.group(1).replace(',', '.'))
            contexto = pergunta[:match.start()].lower()

            if any(term in contexto for term in ['reajuste salarial', 'reajuste de salário', 'reajuste salário', 'salário', 'salario']):
                valores['pct_salario'] = numero
            elif any(term in contexto for term in ['vale alimentação', 'vale alimentacao', 'va ', 'va:']):
                valores['pct_va'] = numero
            elif any(term in contexto for term in ['plr', 'participação nos lucros', 'participacao nos lucros']):
                valores['pct_plr'] = numero
            elif any(term in contexto for term in ['hora extra', 'horas extras', 'he', 'aumento he']):
                valores['pct_he_adicional'] = numero
            else:
                if valores['pct_salario'] == 0.0 and 'reajuste' in contexto:
                    valores['pct_salario'] = numero
                elif valores['pct_salario'] == 0.0 and 'plr' in pergunta.lower():
                    valores['pct_plr'] = numero

        if any(v > 0 for v in valores.values()):
            return valores

        # Fallback para Gemini apenas se nenhum percentual for reconhecido localmente.
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
            return valores

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

            # Identificar todas as tarefas necessárias, incluindo múltiplas frentes de análise.
            tarefas = self._identificar_tarefas(pergunta)
            if len(tarefas) > 1:
                return self._processar_varias_tarefas(tarefas, pergunta, planta_id, arquivos_permitidos)

            intencao = tarefas[0] if tarefas else None
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
            elif intencao == 'RISCO_ML':
                return self._processar_risco_evasao_ml(pergunta, planta_id)
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

    def _processar_simulacao_financeira(self, pergunta, planta_id, variaveis=None):
        """
        Processa simulações financeiras usando o método simular_cenario_completo.

        Args:
            pergunta (str): A pergunta de simulação financeira.
            planta_id (str): O ID da planta.
            variaveis (dict, optional): Percentuais extraídos da pergunta para garantir consistência entre ferramentas.

        Returns:
            dict: Resposta estruturada.
        """
        try:
            # Usar variáveis compartilhadas quando disponíveis
            variaveis = variaveis or self._extrair_variaveis_simulacao(pergunta)

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

            try:
                response = self.client.models.generate_content(model='gemini-flash-latest', contents=prompt_explicacao)
                explicacao = response.text.strip()
            except Exception as e:
                print(f"Gemini indisponível para explicação financeira: {e}")
                explicacao = (
                    "Simulação financeira concluída com sucesso, mas não foi possível gerar a explicação de linguagem natural devido a limitação de quota do modelo. "
                    f"Os percentuais utilizados foram: salário {variaveis['pct_salario']}%, VA {variaveis['pct_va']}%, PLR {variaveis['pct_plr']}%, horas extras {variaveis['pct_he_adicional']}%."
                )

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

    def _detectar_risco_ml(self, pergunta):
        """Detecta consultas de risco, evasão ou turnover que devem usar o modelo ML."""
        texto = pergunta.lower()
        termos = ['risco', 'evasão', 'evasao', 'turnover', 'desligamento', 'rotatividade', 'greve']
        return any(termo in texto for termo in termos)

    def _get_plant_risco_template(self, planta_id):
        """Retorna características de planta para alinhar o chat ao simulador macro."""
        plantas = {
            'G1': {
                'Salário Base': 6000,
                'Age': 35,
                'BusinessTravel': 'Travel_Rarely',
                'Department': 'Sales',
                'Gender': 'Female',
                'JobRole': 'Sales Executive',
                'MaritalStatus': 'Single',
                'OverTime': 'Yes',
            },
            'G2': {
                'Salário Base': 7000,
                'Age': 45,
                'BusinessTravel': 'Travel_Frequently',
                'Department': 'Research & Development',
                'Gender': 'Male',
                'JobRole': 'Laboratory Technician',
                'MaritalStatus': 'Married',
                'OverTime': 'No',
            },
            'G3': {
                'Salário Base': 8500,
                'Age': 50,
                'BusinessTravel': 'Non-Travel',
                'Department': 'Human Resources',
                'Gender': 'Female',
                'JobRole': 'Human Resources',
                'MaritalStatus': 'Divorced',
                'OverTime': 'No',
            }
        }
        return plantas.get(planta_id, {
            'Salário Base': 5290.0,
            'Age': 35,
            'BusinessTravel': 'Travel_Rarely',
            'Department': 'Research & Development',
            'Gender': 'Female',
            'JobRole': 'Research Scientist',
            'MaritalStatus': 'Single',
            'OverTime': 'No',
        })

    def _processar_risco_evasao_ml(self, pergunta, planta_id, variaveis=None):
        """Processa perguntas de risco usando o modelo de ML de turnover."""
        try:
            variaveis = variaveis or self._extrair_variaveis_simulacao(pergunta)
            reajuste_proposto = float(variaveis.get('pct_salario', 0.0) or 0.0)

            modelo = joblib.load('ml/modelo_turnover.pkl')
            template = pd.read_csv('data/ibm_attrition.csv', nrows=1).copy()
            if 'Attrition' in template.columns:
                template = template.drop(columns=['Attrition'])

            planta_info = self._get_plant_risco_template(planta_id)
            salario_base = float(planta_info['Salário Base'])

            template_base = template.copy()
            template_base['MonthlyIncome'] = int(round(salario_base))
            template_base['Age'] = int(planta_info['Age'])
            for feature in ['BusinessTravel', 'Department', 'Gender', 'JobRole', 'MaritalStatus', 'OverTime']:
                if feature in template_base.columns:
                    template_base[feature] = planta_info[feature]
            if 'PercentSalaryHike' in template_base.columns:
                template_base['PercentSalaryHike'] = 0.0

            risco_base = float(modelo.predict_proba(template_base)[0][1])

            novo_salario = salario_base * (1 + reajuste_proposto / 100.0)
            template_ajustado = template_base.copy()
            template_ajustado['MonthlyIncome'] = int(round(novo_salario))
            if 'PercentSalaryHike' in template_ajustado.columns:
                template_ajustado['PercentSalaryHike'] = reajuste_proposto

            risco_ajustado = float(modelo.predict_proba(template_ajustado)[0][1])
            risco_final = risco_ajustado

            resposta = (
                f"O modelo preditivo de ML calculou que, com um reajuste de {reajuste_proposto:.1f}% na planta {planta_id}, "
                f"o risco global de evasão deve passar de {risco_base * 100:.2f}% para {risco_final * 100:.2f}%."
            )

            return {
                "tipo": "RISCO_ML",
                "contexto": planta_id,
                "variaveis_extraidas": variaveis,
                "risco_base": risco_base,
                "risco_ajustado": risco_ajustado,
                "risco_final": risco_final,
                "resposta": resposta
            }
        except Exception as e:
            return {
                "tipo": "RISCO_ML",
                "contexto": planta_id,
                "resposta": f"Não foi possível calcular o risco com o modelo ML. Verifique se o arquivo ml/modelo_turnover.pkl e o template data/ibm_attrition.csv estão disponíveis. Erro: {str(e)}"
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
        except Exception:
            return {
                "tipo": "POLÍTICA",
                "contexto": planta_id,
                "documentos_consultados": [],
                "trechos": [],
                "resposta": "Nenhuma política limitante aplicável encontrada."
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