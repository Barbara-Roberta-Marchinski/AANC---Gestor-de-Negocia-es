import os
import joblib
import pandas as pd
from langfuse.callback import CallbackHandler

from crewai import Agent, Task, Crew, Process, tool


# =====================
# Definição de Ferramentas
# =====================

@tool
def consultar_impacto_financeiro(percentual_reajuste: float) -> str:
    """Simula a consulta de impacto financeiro de um reajuste salarial na folha."""
    custo_extra = percentual_reajuste * 1000000 / 100  # mock simples de cálculo
    return (
        f"Impacto financeiro estimado para {percentual_reajuste:.1f}% de reajuste: "
        f"R$ {custo_extra:,.2f} em custos adicionais na folha de pagamento."
    )


@tool
def consultar_regras_sindicato(topico: str) -> str:
    """Simula a consulta às regras do Acordo Coletivo de Trabalho (ACT)."""
    return (
        f"Regra mock para o tema '{topico}': o ACT estabelece que qualquer reajuste salarial "
        "deve ser negociado com o sindicato, com cláusula de manutenção de emprego e garantia "
        "de aumento real não inferior a 4% para cargos de produção."
    )


@tool("Simulador de Risco ML")
def simular_risco_evasao_ml(planta: str, reajuste_proposto: float) -> str:
    """Simula risco de greve sindical usando o modelo ML e um template oculto do dataset."""
    reajuste_proposto = float(reajuste_proposto)

    modelo = joblib.load("ml/modelo_turnover.pkl")
    template = pd.read_csv("data/ibm_attrition.csv").iloc[[0]].copy()

    salario_base = 5290.0
    novo_salario = salario_base * (1 + reajuste_proposto / 100.0)
    template["MonthlyIncome"] = int(round(novo_salario))

    probabilidades = modelo.predict_proba(template)[0]
    risco_base = float(probabilidades[1]) * 100

    inflacao_esperada = 4.0
    if reajuste_proposto >= inflacao_esperada:
        risco_greve = risco_base - ((reajuste_proposto - inflacao_esperada) * 10.0)
    else:
        risco_greve = risco_base

    risco_greve = max(0.0, min(100.0, risco_greve))

    return (
        f"O modelo avaliou a tensão atual da planta {planta}. "
        f"Considerando a inflação de {inflacao_esperada}%, a proposta de {reajuste_proposto}% "
        f"resulta em um Risco de Greve de {risco_greve:.2f}%.")


# =====================
# Definição dos Agentes
# =====================

analista_remuneracao = Agent(
    name="Analista de Remuneração",
    role="Analista Sênior de Remuneração e Orçamento",
    goal="Calcular o impacto financeiro exato de propostas de reajuste salarial na folha de pagamento.",
    backstory=(
        "Especialista em People Analytics e planejamento financeiro, focado em manter o headcount "
        "dentro do orçamento e identificar riscos de custo para a área de RH."
    ),
    tools=[consultar_impacto_financeiro, simular_risco_evasao_ml],
)

advogado_trabalhista = Agent(
    name="Advogado Trabalhista",
    role="Advogado Especialista em Relações Sindicais",
    goal="Garantir que as propostas financeiras respeitem as cláusulas do Acordo Coletivo de Trabalho.",
    backstory=(
        "Especialista em direito do trabalho, focado em evitar passivos trabalhistas e greves, "
        "assegurando conformidade com o ACT."
    ),
    tools=[consultar_regras_sindicato],
)


# =====================
# Definição das Tarefas
# =====================

task_calcular_custo = Task(
    name="Calcular impacto de reajuste",
    description=(
        "Primeira tarefa: o Analista de Remuneração calcula o custo estimado de um reajuste salarial de 5%."
    ),
    agent=analista_remuneracao,
    input_data={"percentual_reajuste": 5.0},
    step_callbacks=[langfuse_handler],
)

task_revisar_act = Task(
    name="Revisar custo com ACT",
    description=(
        "Segunda tarefa: o Advogado Trabalhista revisa o custo calculado e valida contra as regras do ACT."
    ),
    agent=advogado_trabalhista,
    input_data={"topico": "reajuste salarial e cláusulas de proteção de emprego"},
    step_callbacks=[langfuse_handler],
)


# =====================
# Orquestração com Crew
# =====================

# Configure as variáveis de ambiente LANGFUSE_SECRET_KEY, LANGFUSE_PUBLIC_KEY e LANGFUSE_HOST no .env para habilitar observabilidade.
langfuse_handler = CallbackHandler(
    secret_key=os.getenv("LANGFUSE_SECRET_KEY"),
    public_key=os.getenv("LANGFUSE_PUBLIC_KEY"),
    host=os.getenv("LANGFUSE_HOST"),
)

crew_negociacao = Crew(
    name="CrewNegociacaoColetiva",
    agents=[analista_remuneracao, advogado_trabalhista],
    step_callbacks=[langfuse_handler],
)

processo_negociacao = Process(
    name="ProcessoNegociacaoColetiva",
    crew=crew_negociacao,
    tasks=[task_calcular_custo, task_revisar_act],
)


def main() -> None:
    """Inicia o processo de simulação de negociação coletiva."""
    print("Iniciando o processo de negociação coletiva...")
    resultado = processo_negociacao.kickoff()
    print("Processo finalizado.")
    print("Resultado:")
    print(resultado)


if __name__ == "__main__":
    main()
