"""Aplicação Streamlit do AANC para simulação financeira, chat de dúvidas e filtro por planta."""

import os
import re
import joblib
import streamlit as st
import pandas as pd
from contextlib import nullcontext
from dotenv import load_dotenv
from langfuse import get_client
from src.agent_brain import AANC_Agent

# Configuração da página
st.set_page_config(
    page_title='AANC - Gestor de Negociações Indústria-X',
    page_icon='🤖',
    layout='wide'
)

# Carrega variáveis de ambiente para Langfuse
load_dotenv()

# Inicializa cliente Langfuse para observabilidade, sem quebrar o app se não estiver configurado.
try:
    langfuse_client = get_client()
except Exception:
    langfuse_client = None

# Inicializar agente (uma vez por sessão)
if 'agent' not in st.session_state:
    st.session_state.agent = None

if 'agent' not in st.session_state or st.session_state.agent is None:
    try:
        st.session_state.agent = AANC_Agent()
        st.session_state.agent_initialized = True
        st.session_state.sinonimos_map = st.session_state.agent.dm.get_mapa_sinonimos()
    except Exception as e:
        st.session_state.agent_initialized = False
        st.error(f"Erro ao inicializar o agente: {str(e)}")
else:
    if not hasattr(st.session_state.agent, 'dm') or not hasattr(st.session_state.agent.dm, 'simular_cenario_completo'):
        try:
            st.session_state.agent = AANC_Agent()
            st.session_state.agent_initialized = True
            st.session_state.sinonimos_map = st.session_state.agent.dm.get_mapa_sinonimos()
        except Exception as e:
            st.session_state.agent_initialized = False
            st.error(f"Erro ao reinicializar o agente: {str(e)}")

# Inicializar histórico de chat
if 'messages' not in st.session_state:
    st.session_state.messages = []

@st.cache_resource
def carregar_modelo_turnover(caminho='ml/modelo_turnover.pkl'):
    return joblib.load(caminho)

@st.cache_resource
def carregar_template_turnover(caminho='data/ibm_attrition.csv'):
    df = pd.read_csv(caminho, nrows=1)
    if 'Attrition' in df.columns:
        df = df.drop(columns=['Attrition'])
    return df

# Sidebar
st.sidebar.title("Configurações")

# Seletor de Planta
planta_options = [f"G{i}" for i in range(1, 9)]
planta_id = st.sidebar.selectbox(
    "Selecione a Planta:",
    options=planta_options,
    index=0,
    help="Escolha a planta para filtrar as informações relevantes."
)

# Simulação Financeira
st.sidebar.subheader("Simulação Financeira")
pct_salario_slider = st.sidebar.slider("Reajuste Salarial (%)", 0.0, 20.0, 0.0, 0.5)
pct_salario_input = st.sidebar.number_input("Digite o % do Reajuste Salarial", min_value=0.0, max_value=100.0, value=0.0, step=0.1, format="%.2f")
pct_salario = pct_salario_input if pct_salario_input != 0 else pct_salario_slider

pct_va_slider = st.sidebar.slider("Reajuste VA (%)", 0.0, 20.0, 0.0, 0.5)
pct_va_input = st.sidebar.number_input("Digite o % do Reajuste VA", min_value=0.0, max_value=100.0, value=0.0, step=0.1, format="%.2f")
pct_va = pct_va_input if pct_va_input != 0 else pct_va_slider

pct_plr_slider = st.sidebar.slider("Reajuste PLR (%)", 0.0, 20.0, 0.0, 0.5)
pct_plr_input = st.sidebar.number_input("Digite o % do Reajuste PLR", min_value=0.0, max_value=100.0, value=0.0, step=0.1, format="%.2f")
pct_plr = pct_plr_input if pct_plr_input != 0 else pct_plr_slider

if st.sidebar.button("Executar Simulação", type="primary"):
    if not st.session_state.get('agent_initialized', False):
        st.sidebar.error("Sistema Offline")
    else:
        with st.spinner("Executando simulação..."):
            try:
                if not hasattr(st.session_state.agent, 'dm') or not hasattr(st.session_state.agent.dm, 'simular_cenario_completo'):
                    raise Exception('O agente atual não possui o método de simulação completo. Reinicie o app para recarregá-lo.')

                trace_ctx = nullcontext()
                if langfuse_client is not None:
                    trace_ctx = langfuse_client.start_as_current_observation(
                        as_type="span",
                        name="aanc.macro_simulation",
                        input=f"planta={planta_id},pct_salario={pct_salario},pct_va={pct_va},pct_plr={pct_plr}",
                        metadata={
                            "planta_id": planta_id,
                            "pct_salario": pct_salario,
                            "pct_va": pct_va,
                            "pct_plr": pct_plr,
                        },
                    )

                with trace_ctx as span:
                    resultado = st.session_state.agent.dm.simular_cenario_completo(
                        planta_id=planta_id,
                        pct_salario=pct_salario,
                        pct_va=pct_va,
                        pct_plr=pct_plr,
                        pct_he_adicional=0.0
                    )
                    if span is not None:
                        span.update(
                            output={
                                "status": "success",
                                "simulacao_resultado": resultado,
                            }
                        )
                st.session_state.simulacao_resultado = resultado
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"Erro na simulação: {str(e)}")

if st.sidebar.button("Reiniciar Agente"):
    st.session_state.agent = None
    st.session_state.agent_initialized = False
    st.sidebar.success("Agente reiniciado. Recarregue a página para confirmar.")

# Exibir resultado da simulação
if 'simulacao_resultado' in st.session_state:
    with st.sidebar.expander("Resultado da Simulação"):
        res = st.session_state.simulacao_resultado
        st.metric("Custo Atual Anual", f"R$ {res['Custo Atual']:,.2f}")
        st.metric("Novo Custo Projetado Anual", f"R$ {res['Novo Custo Projetado']:,.2f}")
        st.metric("Impacto Anual Empresa", f"R$ {res['Impacto Anual Empresa']:,.2f}", delta=f"{res['Impacto Anual Empresa']:,.2f}")

        detalhes = res.get('Detalhes', {})
        composicao = {
            'Tipo': ['Salário', 'Salário com encargos', 'PLR', 'VA'],
            'Custo Atual Anual': [
                detalhes.get('Salário Base Atual', 0) * 12,
                detalhes.get('Custo Salário Atual Anual', 0),
                detalhes.get('Custo PLR Atual Anual', 0),
                detalhes.get('Custo VA Atual Anual', 0)
            ],
            'Novo Custo Projetado Anual': [
                detalhes.get('Novo Salário', 0) * 12,
                detalhes.get('Custo Salário Projetado Anual', 0),
                detalhes.get('Custo PLR Projetado Anual', 0),
                detalhes.get('Custo VA Projetado Anual', 0)
            ]
        }
        df_composicao = pd.DataFrame(composicao)
        df_composicao['Incremental Anual'] = df_composicao['Novo Custo Projetado Anual'] - df_composicao['Custo Atual Anual']
        st.subheader("Composição por Tipo (Anual)")
        st.dataframe(df_composicao.style.format({
            'Custo Atual Anual': 'R$ {:,.2f}',
            'Novo Custo Projetado Anual': 'R$ {:,.2f}',
            'Incremental Anual': 'R$ {:,.2f}'
        }))

# Botão para limpar histórico
if st.sidebar.button("Limpar Histórico", type="secondary"):
    st.session_state.messages = []
    if 'simulacao_resultado' in st.session_state:
        del st.session_state.simulacao_resultado
    st.rerun()

# Indicador de status
if st.session_state.get('agent_initialized', False):
    st.sidebar.success("Sistema Online")
else:
    st.sidebar.error("Sistema Offline")

# Corpo principal por abas
main_tab, turnover_tab, simulador_macro_tab = st.tabs(["Chat", "🔮 Previsão de Turnover", "⚖️ Simulador de Negociação (Macro)"])

with turnover_tab:
    st.header("🔮 Previsão de Turnover")
    st.markdown("Use a linha de template do dataset `data/ibm_attrition.csv` e ajuste apenas estas 5 variáveis para calcular a probabilidade de turnover.")

    model_error = None
    try:
        modelo_turnover = carregar_modelo_turnover()
    except Exception as e:
        modelo_turnover = None
        model_error = str(e)

    template_error = None
    try:
        template_df = carregar_template_turnover()
    except Exception as e:
        template_df = None
        template_error = str(e)

    if model_error:
        st.error(f"Erro ao carregar modelo: {model_error}")
    elif template_error:
        st.error(f"Erro ao carregar template: {template_error}")
    elif template_df is None or template_df.empty:
        st.error("Não foi possível carregar o template base para previsão de turnover.")
    else:
        base = template_df.iloc[0].copy()
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            age = st.slider("Age", min_value=18, max_value=65, value=int(base.get('Age', 30)))
        with col2:
            monthly_income = st.number_input(
                "MonthlyIncome",
                min_value=0,
                max_value=1000000,
                value=int(base.get('MonthlyIncome', 5000)),
                step=100,
                format="%d"
            )
        with col3:
            overtime = st.selectbox(
                "OverTime",
                options=["Yes", "No"],
                index=0 if str(base.get('OverTime', 'No')) == 'Yes' else 1
            )
        with col4:
            job_satisfaction = st.slider(
                "JobSatisfaction",
                min_value=1,
                max_value=4,
                value=int(base.get('JobSatisfaction', 1))
            )
        with col5:
            total_working_years = st.slider(
                "TotalWorkingYears",
                min_value=0,
                max_value=60,
                value=int(base.get('TotalWorkingYears', 0))
            )

        if st.button("Calcular Risco"):
            predict_df = template_df.copy()
            predict_df.at[predict_df.index[0], 'Age'] = age
            predict_df.at[predict_df.index[0], 'MonthlyIncome'] = monthly_income
            predict_df.at[predict_df.index[0], 'OverTime'] = overtime
            predict_df.at[predict_df.index[0], 'JobSatisfaction'] = job_satisfaction
            predict_df.at[predict_df.index[0], 'TotalWorkingYears'] = total_working_years

            if 'Attrition' in predict_df.columns:
                predict_df = predict_df.drop(columns=['Attrition'])

            try:
                proba = modelo_turnover.predict_proba(predict_df)[0][1]
                risco_pct = float(proba) * 100

                if risco_pct > 50:
                    st.error(f"Risco de Turnover: {risco_pct:.1f}%")
                else:
                    st.success(f"Risco de Turnover: {risco_pct:.1f}%")

                st.markdown("### Template usado para previsão")
                st.dataframe(predict_df)
            except Exception as e:
                st.error(f"Erro ao calcular o risco: {e}")

with simulador_macro_tab:
    st.header("⚖️ Simulador de Negociação (Macro)")
    st.markdown("Simule o risco global de evasão de uma planta inteira com base em uma proposta de reajuste sindical.")

    modelo_macro = None
    template_macro = None
    model_error_macro = None
    template_error_macro = None

    try:
        modelo_macro = carregar_modelo_turnover()
    except Exception as e:
        model_error_macro = str(e)

    try:
        template_macro = carregar_template_turnover()
    except Exception as e:
        template_error_macro = str(e)

    if model_error_macro:
        st.error(f"Erro ao carregar modelo: {model_error_macro}")
    elif template_error_macro:
        st.error(f"Erro ao carregar template: {template_error_macro}")
    elif template_macro is None or template_macro.empty:
        st.error("Não foi possível carregar o template oculto para a simulação macro.")
    else:
        plantas_macro = {
            "G1": {
                "Salário Base": 6000,
                "Idade Média": 35,
                "BusinessTravel": "Travel_Rarely",
                "Department": "Sales",
                "Gender": "Female",
                "JobRole": "Sales Executive",
                "MaritalStatus": "Single",
                "OverTime": "Yes",
            },
            "G2": {
                "Salário Base": 7000,
                "Idade Média": 45,
                "BusinessTravel": "Travel_Frequently",
                "Department": "Research & Development",
                "Gender": "Male",
                "JobRole": "Laboratory Technician",
                "MaritalStatus": "Married",
                "OverTime": "No",
            },
            "G3": {
                "Salário Base": 8500,
                "Idade Média": 50,
                "BusinessTravel": "Non-Travel",
                "Department": "Human Resources",
                "Gender": "Female",
                "JobRole": "Human Resources",
                "MaritalStatus": "Divorced",
                "OverTime": "No",
            },
        }

        planta_macro = st.selectbox("Planta", list(plantas_macro.keys()), index=0)
        reajuste_macro = st.slider("Reajuste Proposto (%)", 0.0, 15.0, 0.0, 0.5)

        dados_macro = plantas_macro[planta_macro]
        salario_base_macro = dados_macro["Salário Base"]
        idade_media_macro = dados_macro["Idade Média"]
        novo_salario_macro = salario_base_macro * (1 + reajuste_macro / 100)

        predict_df_macro = template_macro.copy()
        predict_df_macro.at[predict_df_macro.index[0], "Age"] = int(idade_media_macro)
        predict_df_macro.at[predict_df_macro.index[0], "MonthlyIncome"] = int(round(novo_salario_macro))
        predict_df_macro.at[predict_df_macro.index[0], "PercentSalaryHike"] = int(round(reajuste_macro))
        predict_df_macro.at[predict_df_macro.index[0], "BusinessTravel"] = dados_macro["BusinessTravel"]
        predict_df_macro.at[predict_df_macro.index[0], "Department"] = dados_macro["Department"]
        predict_df_macro.at[predict_df_macro.index[0], "Gender"] = dados_macro["Gender"]
        predict_df_macro.at[predict_df_macro.index[0], "JobRole"] = dados_macro["JobRole"]
        predict_df_macro.at[predict_df_macro.index[0], "MaritalStatus"] = dados_macro["MaritalStatus"]
        predict_df_macro.at[predict_df_macro.index[0], "OverTime"] = dados_macro["OverTime"]

        if "MonthlyRate" in predict_df_macro.columns:
            predict_df_macro.at[predict_df_macro.index[0], "MonthlyRate"] = int(round(predict_df_macro.at[predict_df_macro.index[0], "MonthlyRate"] * (1 + reajuste_macro / 100)))

        try:
            predict_df_base = template_macro.copy()
            predict_df_base.at[predict_df_base.index[0], "Age"] = int(idade_media_macro)
            predict_df_base.at[predict_df_base.index[0], "MonthlyIncome"] = int(round(salario_base_macro))
            predict_df_base.at[predict_df_base.index[0], "PercentSalaryHike"] = 0
            for feature in ["BusinessTravel", "Department", "Gender", "JobRole", "MaritalStatus", "OverTime"]:
                if feature in predict_df_base.columns:
                    predict_df_base.at[predict_df_base.index[0], feature] = dados_macro[feature]

            if "MonthlyRate" in predict_df_base.columns:
                predict_df_base.at[predict_df_base.index[0], "MonthlyRate"] = int(round(predict_df_base.at[predict_df_base.index[0], "MonthlyRate"]))

            risco_base = float(modelo_macro.predict_proba(predict_df_base)[0][1]) * 100
            risco_ajustado = float(modelo_macro.predict_proba(predict_df_macro)[0][1]) * 100
            fallback_applied = False

            if reajuste_macro > 0 and abs(risco_ajustado - risco_base) < 0.01:
                ajuste_heuristico = min(risco_base, reajuste_macro * 0.003)
                risco_ajustado = max(0.0, risco_base - ajuste_heuristico)
                fallback_applied = True

            col1, col2, col3 = st.columns(3)
            col1.metric("Reajuste Proposto", f"{reajuste_macro:.1f}%")
            col2.metric("Novo Salário Médio", f"R$ {novo_salario_macro:,.0f}")
            col3.metric("Risco de Greve/Evasão Projetado", f"{risco_ajustado:.1f}%")

            if fallback_applied:
                st.info("O modelo ML apresentou pouca variação nesse intervalo. Foi aplicado um ajuste heurístico para refletir o impacto do reajuste salarial.")

            st.progress(min(1.0, risco_ajustado / 100))

            if risco_ajustado > 40:
                st.error("🚨 ALTO RISCO DE GREVE/EVASÃO")
            elif risco_ajustado > 20:
                st.warning("⚠️ RISCO MODERADO (Tensão na Base)")
            else:
                st.success("✅ MARGEM SEGURA (Estabilidade)")

            st.markdown("### Detalhes da simulação")
            st.write(f"- Planta simulada: **{planta_macro}**")
            st.write(f"- Salário Base atual: **R$ {salario_base_macro:,.0f}**")
            st.write(f"- Idade Média usada: **{idade_media_macro} anos**")
            st.write(f"- Novo Salário Médio projetado: **R$ {novo_salario_macro:,.0f}**")
            st.write(f"- Risco base ML: **{risco_base:.1f}%**")
            st.write(f"- Risco ajustado ML: **{risco_ajustado:.1f}%**")
        except Exception as e:
            st.error(f"Erro ao calcular o risco macro: {e}")

with main_tab:
    st.title("🤖 AANC - Gestor de Negociações Indústria-X")
    st.markdown("Sistema inteligente para consultas sobre negociações trabalhistas e cálculos de RH.")

    # Interface de chat
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if "tipo" in message:
                st.caption(f"Tipo: {message['tipo']} | Contexto: {message['contexto']}")

    # Input do usuário
    if prompt := st.chat_input("Digite sua pergunta sobre negociações ou cálculos..."):
        if not st.session_state.get('agent_initialized', False):
            st.error("Sistema em manutenção temporária. Por favor, tente em instantes.")
        else:
            # Detectar sinônimos de planta no texto da pergunta
            texto_pergunta = prompt.lower()
            planta_override = None
            for sinonimo, planta_destino in (st.session_state.sinonimos_map or {}).items():
                if re.search(rf'\b{re.escape(sinonimo)}\b', texto_pergunta):
                    planta_override = planta_destino
                    break

            contexto_planta = planta_override or planta_id

            trace_ctx = nullcontext()
            if langfuse_client is not None:
                trace_ctx = langfuse_client.start_as_current_observation(
                    as_type="span",
                    name="aanc.chat_input",
                    input=prompt,
                    metadata={
                        "planta_id": contexto_planta,
                        "tab": "chat",
                    },
                )

            with trace_ctx as span:
                # Adicionar mensagem do usuário
                st.session_state.messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.markdown(prompt)

                if planta_override:
                    st.info(f"Contexto de planta definido por sinônimo: {planta_override}")

                # Processar pergunta
                with st.chat_message("assistant"):
                    try:
                        with st.spinner("Processando sua pergunta..."):
                            resultado = st.session_state.agent.processar_pergunta(prompt, contexto_planta)

                        # Verificar indicador de risco
                        resposta_texto = resultado.get('resposta', '').lower()
                        risco_alto = (
                            'greve' in resposta_texto or
                            'paralisação' in resposta_texto or
                            any(f'{i}%' in resposta_texto for i in range(6, 101))  # impacto >5%
                        )

                        if risco_alto:
                            st.error("⚠️ **ALTO RISCO** - Esta resposta pode indicar impactos significativos. Consulte especialistas.")
                        elif 'erro' in resultado.get('tipo', '').lower():
                            st.warning("⚠️ **ATENÇÃO** - Verifique os detalhes da resposta.")
                        else:
                            st.success("✅ **BAIXO RISCO** - Resposta dentro dos parâmetros normais.")

                        # Exibir resposta
                        st.markdown(f"**Tipo:** {resultado.get('tipo', 'N/A')}")
                        st.markdown(f"**Contexto:** {resultado.get('contexto', 'N/A')}")

                        if 'documentos_consultados' in resultado:
                            st.markdown(f"**Documentos Consultados:** {', '.join(resultado['documentos_consultados'])}")

                        if 'query_sql' in resultado:
                            with st.expander("Ver Query SQL"):
                                st.code(resultado['query_sql'], language='sql')

                        if 'resultado' in resultado:
                            with st.expander("Resultado da Consulta"):
                                st.dataframe(resultado['resultado'])

                        if 'variaveis_extraidas' in resultado:
                            with st.expander("Variáveis Extraídas"):
                                st.json(resultado['variaveis_extraidas'])

                        if 'resultado_simulacao' in resultado:
                            with st.expander("Detalhes da Simulação"):
                                sim = resultado['resultado_simulacao']
                                st.metric("Custo Atual Anual", f"R$ {sim['Custo Atual']:,.2f}")
                                st.metric("Novo Custo Projetado Anual", f"R$ {sim['Novo Custo Projetado']:,.2f}")
                                st.metric("Impacto Anual Empresa", f"R$ {sim['Impacto Anual Empresa']:,.2f}", delta=f"{sim['Impacto Anual Empresa']:,.2f}")

                                detalhes = sim.get('Detalhes', {})
                                composicao = {
                                    'Tipo': ['Salário', 'PLR', 'VA'],
                                    'Custo Atual': [
                                        detalhes.get('Salário Base Atual', 0),
                                        detalhes.get('PLR Atual', 0),
                                        detalhes.get('VA Atual', 0)
                                    ],
                                    'Novo Custo': [
                                        detalhes.get('Novo Salário', 0),
                                        detalhes.get('Novo PLR', 0),
                                        detalhes.get('Novo VA', 0)
                                    ]
                                }
                                df_composicao = pd.DataFrame(composicao)
                                df_composicao['Incremental'] = df_composicao['Novo Custo'] - df_composicao['Custo Atual']
                                st.subheader("Composição do Custo")
                                st.dataframe(df_composicao.style.format({
                                    'Custo Atual': 'R$ {:,.2f}',
                                    'Novo Custo': 'R$ {:,.2f}',
                                    'Incremental': 'R$ {:,.2f}'
                                }))

                        if 'benchmark_dados' in resultado:
                            with st.expander("Dados de Benchmark de Mercado"):
                                benchmark_df = pd.DataFrame(resultado['benchmark_dados'])
                                st.dataframe(benchmark_df)

                                if 'nossa_pratica' in resultado and resultado['nossa_pratica']:
                                    st.subheader("Nossa Prática Atual")
                                    pratica_df = pd.DataFrame(resultado['nossa_pratica'])
                                    st.dataframe(pratica_df.style.format({
                                        'salario_medio': 'R$ {:,.2f}',
                                        'va_medio': 'R$ {:,.2f}',
                                        'plr_medio': 'R$ {:,.2f}'
                                    }))

                        st.markdown("### Resposta:")
                        st.markdown(resultado.get('resposta', 'Nenhuma resposta gerada.'))

                        # Exibição específica para CÁLCULO_FINANCEIRO
                        if resultado.get('tipo') == 'RISCO_ML' and 'risco_final' in resultado:
                            st.metric("Risco Global de Evasão", f"{resultado['risco_final'] * 100:.2f}%")

                        if resultado.get('tipo') == 'CÁLCULO_FINANCEIRO' and 'resultado_simulacao' in resultado:
                            sim = resultado['resultado_simulacao']
                            custo_atual = sim['Custo Atual']
                            impacto_anual = sim['Impacto Anual Empresa']
                            pct_aumento = (impacto_anual / custo_atual) * 100 if custo_atual > 0 else 0

                            # Métricas principais
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Custo Incremental Total Anual", f"R$ {impacto_anual:,.2f}")
                            with col2:
                                st.metric("% de Aumento no Budget", f"{pct_aumento:.2f}%")

                            # Alerta de risco se >5%
                            if pct_aumento > 5:
                                st.error(f"🚨 **RISCO FINANCEIRO ELEVADO** - O aumento de {pct_aumento:.2f}% no budget da planta {planta_id} excede 5%. Recomenda-se revisão cuidadosa das premissas e consulta aos stakeholders.")

                        # Adicionar ao histórico
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": resultado.get('resposta', 'Nenhuma resposta gerada.'),
                            "tipo": resultado.get('tipo', 'N/A'),
                            "contexto": resultado.get('contexto', 'N/A')
                        })

                        if span is not None:
                            span.update(
                                output=resultado.get('resposta', resultado),
                                metadata={
                                    "tipo": resultado.get('tipo', 'N/A'),
                                    "contexto": resultado.get('contexto', 'N/A'),
                                },
                            )
                    except Exception as e:
                        error_msg = "Sistema em manutenção temporária. Por favor, tente em instantes."
                        st.error(error_msg)
                        st.session_state.messages.append({"role": "assistant", "content": error_msg})