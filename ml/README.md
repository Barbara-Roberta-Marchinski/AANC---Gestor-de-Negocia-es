# 🤖 AANC - Agente de Apoio à Negociação

## 📖 Descrição do Projeto

O AANC é um Agente de Apoio à Negociação desenvolvido para ajudar na análise de cenários trabalhistas e negociações sindicais em uma planta industrial.
O projeto combina:
- consultas determinísticas em SQL usando DuckDB,
- simulação financeira de impactos salariais e de benefícios,
- **previsões de risco (turnover/evasão) utilizando modelos de Machine Learning,**
- busca semântica em documentação sindical via RAG (Retrieval Augmented Generation),
- geração de respostas em linguagem natural com Gemini / Google GenAI,
- filtros de segurança corporativa e prevenção contra alucinações (Guardrails).

## 🏗️ Arquitetura

A arquitetura do projeto é híbrida e conta com múltiplos motores analíticos:
- **Modelo Preditivo (Machine Learning)**: Pipeline treinado com Scikit-Learn (`ml/modelo_turnover.pkl`) que projeta o risco de greve e evasão com base em propostas salariais e dados históricos (dataset IBM HR).
- **SQL Determinístico**: O módulo `src/database_manager.py` gerencia dados estruturados em DuckDB e executa cálculos de custos por planta.
- **RAG (Retrieval Augmented Generation)**: O módulo `src/rag_engine.py` indexa documentos PDF e realiza busca por contexto relevante com `txtai`.
- **Camada de Orquestração**: O módulo `src/agent_brain.py` integra todos os motores (Cálculo, ML e RAG) e roteia as perguntas inteligentemente.
- **Camada de Segurança (Guardrails)**: O módulo `src/safety_layer.py` atua como um firewall, interceptando *Prompt Injections* e evitando respostas fora do escopo de RH.
- **Interface de Usuário**: `app.py` oferece uma aplicação Streamlit para seleção de planta, simulações preditivas e chat inteligente.

## ✨ Principais Funcionalidades

- Cálculo de impacto financeiro por planta com:
  - reajuste salarial,
  - reajuste de VA,
  - reajuste de PLR,
  - horas extras (HE) e encargos sociais.
- Exibição de custos anuais com e sem encargos.
- Busca por contexto em documentos de negociação sindical filtrada por planta.
- Classificação automática de intenção entre cálculo, política e simulação financeira.
- Sistema de chat que responde com base em documentos e em cálculos estruturados, protegido contra solicitações indevidas.
- Simulador macro de negociação para estimar risco de greve/evasão com base em aumento salarial proposto para uma planta inteira utilizando inferência de Machine Learning.

## 📂 Estrutura de Arquivos

- `app.py` - interface Streamlit do projeto.
- `src/database_manager.py` - gerencia DuckDB, tabelas e simulações de custo.
- `src/rag_engine.py` - indexa documentos e realiza buscas semânticas.
- `src/agent_brain.py` - agente principal que orquestra SQL, RAG, ML e Gemini.
- `src/safety_layer.py` - pipeline de segurança e sanitização de inputs.
- `src/agentes_negociacao.py` - agentes autônomos utilizando a estrutura CrewAI.
- `data/` - dados de entrada em CSV (incluindo base ibm_attrition).
- `docs/` - documentos PDF para indexação e busca.
- `ml/` - scripts e modelos serializados de Machine Learning (.pkl).
- `organize_structure.py` - script de criação de pastas e arquivos iniciais.

## ⚖️ Simulador de Negociação (Macro)

A nova aba `⚖️ Simulador de Negociação (Macro)` em `app.py` permite:

- selecionar uma planta mock (`G1`, `G2`, `G3`);
- aplicar um percentual de reajuste salarial entre `0%` e `15%`;
- calcular o novo salário médio da planta;
- prever um risco projetado de greve/evasão baseado no modelo `ml/modelo_turnover.pkl`;
- exibir o resultado em métricas e alertas de cor com base no nível de risco.

O simulador utiliza um template oculto do dataset `data/ibm_attrition.csv` para manter os inputs do modelo consistentes.
- O cálculo de risco foi ajustado para refletir que um aumento salarial macro deve reduzir o risco projetado, usando uma suavização baseada no reajuste aplicado.

## ⚙️ Configuração do Ambiente

1. Crie e ative o ambiente virtual:

```bash
python -m venv env_aanc
source env_aanc/bin/activate  # Linux/macOS
env_aanc\Scripts\activate.bat # Windows PowerShell