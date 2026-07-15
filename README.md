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

---

## 🏗️ Arquitetura

A arquitetura do projeto é híbrida e conta com múltiplos motores analíticos:
- **Modelo Preditivo (Machine Learning)**: Pipeline treinado com Scikit-Learn (`ml/modelo_turnover.pkl`) que projeta o risco de greve e evasão com base em propostas salariais e dados históricos (dataset IBM HR).
- **SQL Determinístico**: O módulo `src/database_manager.py` gerencia dados estruturados em DuckDB e executa cálculos de custos por planta.
- **RAG (Retrieval Augmented Generation)**: O módulo `src/rag_engine.py` indexa documentos PDF e realiza busca por contexto relevante com `txtai`.
- **Camada de Orquestração**: O módulo `src/agent_brain.py` integra todos os motores (Cálculo, ML e RAG) e roteia as perguntas inteligentemente.
- **Agentes Autônomos**: Integração com CrewAI (`src/agentes_negociacao.py`) para simulações de debates e análises aprofundadas.
- **Camada de Segurança (Guardrails)**: O módulo `src/safety_layer.py` atua como um firewall, interceptando *Prompt Injections* e evitando respostas fora do escopo de RH.
- **Interface de Usuário**: `app.py` oferece uma aplicação Streamlit para seleção de planta, simulações preditivas e chat inteligente.

---

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

---

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

---

## ⚖️ Simulador de Negociação (Macro)

A nova aba `⚖️ Simulador de Negociação (Macro)` em `app.py` permite:

- selecionar uma planta mock (`G1`, `G2`, `G3`);
- aplicar um percentual de reajuste salarial entre `0%` e `15%`;
- calcular o novo salário médio da planta;
- prever um risco projetado de greve/evasão baseado no modelo `ml/modelo_turnover.pkl`;
- exibir o resultado em métricas e alertas de cor com base no nível de risco.

O simulador utiliza um template oculto do dataset `data/ibm_attrition.csv` para manter os inputs do modelo consistentes.
- O cálculo de risco foi ajustado para refletir que um aumento salarial macro deve reduzir o risco projetado, usando uma suavização baseada no reajuste aplicado.

---

## ⚙️ Configuração do Ambiente

1. Crie e ative o ambiente virtual:

```bash
python -m venv env_aanc
source env_aanc/bin/activate  # Linux/macOS
env_aanc\Scripts\activate.bat # Windows PowerShell
Instale as dependências essenciais:

Bash
pip install -r requirements.txt
Para execução de testes de auditoria, instale também deepeval:

Bash
pip install deepeval
Crie o arquivo .env na raiz do projeto e adicione a chave da API e monitoramento:

Plaintext
GOOGLE_API_KEY=your_api_key_here
LANGFUSE_PUBLIC_KEY=your_public_key
LANGFUSE_SECRET_KEY=your_secret_key
LANGFUSE_HOST=[https://cloud.langfuse.com](https://cloud.langfuse.com)
HF_TOKEN=your_huggingface_token
Inicie a aplicação Streamlit:

Bash
streamlit run app.py
🛡️ Segurança e LGPD
Este projeto foi desenvolvido com foco em privacidade e conformidade:

usa dados sintéticos para testes e demonstrações.

mantém chaves de API fora do controle de versão via .env e .gitignore.

foi pensado para ser compatível com modelos locais e ambientes fechados, reduzindo o risco de exposição de dados sensíveis.

a lógica de consulta e simulação trabalha com dados estruturados e não depende de armazenamento persistente de informações pessoais.

Implementa um Security Pipeline que bloqueia tentativas de uso da IA para fins não laborais/administrativos.

📊 Observabilidade e Testes
app.py possui instrumentação Langfuse para capturar spans de chat e simulações macro usando start_as_current_observation().

src/agentes_negociacao.py inclui callbacks de Langfuse nos passos da CrewAI para rastrear a execução do agente de negociação.

test_auditoria_agentes.py é uma suíte de auditoria que valida a qualidade das respostas do agente com DeepEval. A suíte agora usa um JuizGemini local que simula um modelo de avaliação e cobre um golden dataset de 15 perguntas e respostas esperadas.

O teste garante a relevância e fidelidade das respostas através de métricas de AnswerRelevancyMetric e FaithfulnessMetric, quando disponíveis.

Para executar os testes:

Bash
pip install deepeval
python -m pytest -q test_auditoria_agentes.py
Se deepeval não estiver instalado, o teste será ignorado automaticamente.

💡 Observações
Os cálculos de custo aplicam provisões de férias e 13° e encargos sociais apenas ao salário, enquanto VA e PLR são tratados como valores de benefício sem encargos diretos.

Os custos mensais de salário e VA são convertidos para base anual para comparação homogênea com PLR.

🔬 Treinamento de Modelo de Turnover (Machine Learning)
O projeto também inclui um script de machine learning para prever turnover com base no dataset público IBM HR Analytics.

Script: ml/treinar_modelo.py

Guia de uso: ml/README.md

Comando para executar:

Bash
python ml/treinar_modelo.py
O script treina um modelo de classificação Random Forest/Logistic Regression, avalia métricas importantes e salva o pipeline treinado em ml/modelo_turnover.pkl.

Passo a passo rápido para treinar e validar o modelo
Ative o ambiente virtual:

Bash
env_aanc\Scripts\activate.bat
Instale as dependências do projeto, se ainda não tiver feito:

Bash
pip install -r requirements.txt
Execute o treinamento do modelo:

Bash
python ml/treinar_modelo.py
Verifique se o arquivo ml/modelo_turnover.pkl foi gerado com sucesso.

Inicie a aplicação Streamlit somente após a geração do modelo:

Bash
streamlit run app.py
O modelo foi atualizado para suportar a nova aba de simulação macro e reflete a redução de risco quando o reajuste salarial é maior.

Observação: a aba ⚖️ Simulador de Negociação (Macro) depende do arquivo ml/modelo_turnover.pkl para calcular o risco de greve/evasão.