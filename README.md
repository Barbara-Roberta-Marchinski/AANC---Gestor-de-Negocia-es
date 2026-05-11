# AANC - Agente de Apoio à Negociação

## Descrição do Projeto

O AANC é um Agente de Apoio à Negociação desenvolvido para ajudar na análise de cenários trabalhistas e negociações sindicais em uma planta industrial.
O projeto combina:
- consultas determinísticas em SQL usando DuckDB,
- simulação financeira de impactos salariais e de benefícios,
- busca semântica em documentação sindical via RAG (Retrieval Augmented Generation),
- geração de respostas em linguagem natural com Gemini / Google GenAI.

## Arquitetura

A arquitetura do projeto é híbrida:
- **SQL Determinístico**: O módulo `src/database_manager.py` gerencia dados estruturados em DuckDB e executa cálculos de custos por planta.
- **RAG (Retrieval Augmented Generation)**: O módulo `src/rag_engine.py` indexa documentos PDF e realiza busca por contexto relevante com `txtai`.
- **Camada de Orquestração**: O módulo `src/agent_brain.py` integra os dois motores e roteia perguntas entre cálculo financeiro, consulta SQL e respostas baseadas em políticas/documentos.
- **Interface de Usuário**: `app.py` oferece uma aplicação Streamlit para seleção de planta, simulação de reajustes e chat inteligente.

## Principais Funcionalidades

- Cálculo de impacto financeiro por planta com:
  - reajuste salarial,
  - reajuste de VA,
  - reajuste de PLR,
  - horas extras (HE) e encargos sociais.
- Exibição de custos anuais com e sem encargos.
- Busca por contexto em documentos de negociação sindical filtrada por planta.
- Classificação automática de intenção entre cálculo, política e simulação financeira.
- Sistema de chat que responde com base em documentos e em cálculos estruturados.

## Estrutura de Arquivos

- `app.py` - interface Streamlit do projeto.
- `src/database_manager.py` - gerencia DuckDB, tabelas e simulações de custo.
- `src/rag_engine.py` - indexa documentos e realiza buscas semânticas.
- `src/agent_brain.py` - agente principal que orquestra SQL, RAG e Gemini.
- `data/` - dados de entrada em CSV.
- `docs/` - documentos PDF para indexação e busca.
- `organize_structure.py` - script de criação de pastas e arquivos iniciais.

## Configuração do Ambiente

1. Crie e ative o ambiente virtual:

```bash
python -m venv env_aanc
source env_aanc/bin/activate  # Linux/macOS
env_aanc\Scripts\activate.bat # Windows PowerShell
```

2. Instale as dependências:

```bash
pip install duckdb pandas streamlit txtai PyPDF2 python-dotenv google-genai
```

3. Crie o arquivo `.env` na raiz do projeto e adicione a chave da API:

```text
GOOGLE_API_KEY=your_api_key_here
```

4. Inicie a aplicação Streamlit:

```bash
streamlit run app.py
```

## Segurança e LGPD

Este projeto foi desenvolvido com foco em privacidade e conformidade:
- usa **dados sintéticos** para testes e demonstrações.
- mantém chaves de API fora do controle de versão via `.env` e `.gitignore`.
- foi pensado para ser compatível com **modelos locais** e ambientes fechados, reduzindo o risco de exposição de dados sensíveis.
- a lógica de consulta e simulação trabalha com dados estruturados e não depende de armazenamento persistente de informações pessoais.

## Observações

- Os cálculos de custo aplicam encargos sociais apenas ao salário, enquanto VA e PLR são tratados como valores de benefício sem encargos diretos.
- Os custos mensais de salário e VA são convertidos para base anual para comparação homogênea com PLR.
