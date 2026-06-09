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
        "de aumento real não inferior a 4% para cargos de produção."")


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
    tools=[consultar_impacto_financeiro],
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
)

task_revisar_act = Task(
    name="Revisar custo com ACT",
    description=(
        "Segunda tarefa: o Advogado Trabalhista revisa o custo calculado e valida contra as regras do ACT."
    ),
    agent=advogado_trabalhista,
    input_data={"topico": "reajuste salarial e cláusulas de proteção de emprego"},
)


# =====================
# Orquestração com Crew
# =====================

crew_negociacao = Crew(
    name="CrewNegociacaoColetiva",
    agents=[analista_remuneracao, advogado_trabalhista],
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
