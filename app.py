"""Aplicação Streamlit do AANC para simulação financeira, chat de dúvidas e filtro por planta."""

import streamlit as st
import pandas as pd
from src.agent_brain import AANC_Agent

# Configuração da página
st.set_page_config(
    page_title='AANC - Gestor de Negociações Indústria-X',
    page_icon='🤖',
    layout='wide'
)

# Inicializar agente (uma vez por sessão)
if 'agent' not in st.session_state:
    st.session_state.agent = None

if 'agent' not in st.session_state or st.session_state.agent is None:
    try:
        st.session_state.agent = AANC_Agent()
        st.session_state.agent_initialized = True
    except Exception as e:
        st.session_state.agent_initialized = False
        st.error(f"Erro ao inicializar o agente: {str(e)}")
else:
    if not hasattr(st.session_state.agent, 'dm') or not hasattr(st.session_state.agent.dm, 'simular_cenario_completo'):
        try:
            st.session_state.agent = AANC_Agent()
            st.session_state.agent_initialized = True
        except Exception as e:
            st.session_state.agent_initialized = False
            st.error(f"Erro ao reinicializar o agente: {str(e)}")

# Inicializar histórico de chat
if 'messages' not in st.session_state:
    st.session_state.messages = []

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

                resultado = st.session_state.agent.dm.simular_cenario_completo(
                    planta_id=planta_id,
                    pct_salario=pct_salario,
                    pct_va=pct_va,
                    pct_plr=pct_plr,
                    pct_he_adicional=0.0
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

# Corpo principal
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
        # Adicionar mensagem do usuário
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Processar pergunta
        with st.chat_message("assistant"):
            with st.spinner("Processando sua pergunta..."):
                try:
                    resultado = st.session_state.agent.processar_pergunta(prompt, planta_id)

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

                    st.markdown("### Resposta:")
                    st.markdown(resultado.get('resposta', 'Nenhuma resposta gerada.'))

                    # Exibição específica para CÁLCULO_FINANCEIRO
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

                except Exception as e:
                    error_msg = "Sistema em manutenção temporária. Por favor, tente em instantes."
                    st.error(error_msg)
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})