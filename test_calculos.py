from src.database_manager import DataManager

# Testar os novos cálculos
dm = DataManager()
dm.inicializar_tabelas()

# Verificar plantas disponíveis
plantas_df = dm.executar_consulta("SELECT DISTINCT Planta FROM headcount")
print("Plantas disponíveis:")
print(plantas_df)

# Usar a primeira planta disponível
if not plantas_df.empty:
    planta_id = plantas_df.iloc[0, 0]
    print(f"\nUsando planta: {planta_id}")

    # Simular cenário com reajustes
    resultado = dm.simular_cenario_completo(
        planta_id=planta_id,
        pct_salario=5.0,
        pct_va=10.0,
        pct_plr=15.0,
        pct_he_adicional=0.0
    )

    print("Resultado da Simulação:")
    print(f"Chaves disponíveis: {list(resultado.keys())}")
    print(f"Custo Atual: R$ {resultado['Custo Atual']:,.2f}")
    print(f"Novo Custo Projetado: R$ {resultado['Novo Custo Projetado']:,.2f}")
    print(f"Impacto Anual Empresa: R$ {resultado['Impacto Anual Empresa']:,.2f}")
    if 'Impacto Nominal (Folha)' in resultado:
        print(f"Impacto Nominal (Folha): R$ {resultado['Impacto Nominal (Folha)']:,.2f}")
    else:
        print("Impacto Nominal (Folha) não encontrado")

    print("\nDetalhes:")
    detalhes = resultado['Detalhes']
    print(f"Salário Base Atual: R$ {detalhes['Salário Base Atual']:,.2f}")
    print(f"Novo Salário: R$ {detalhes['Novo Salário']:,.2f}")
    print(f"PLR Atual: R$ {detalhes['PLR Atual']:,.2f}")
    print(f"Novo PLR: R$ {detalhes['Novo PLR']:,.2f}")
    print(f"VA Atual: R$ {detalhes['VA Atual']:,.2f}")
    print(f"Novo VA: R$ {detalhes['Novo VA']:,.2f}")
else:
    print("Nenhuma planta encontrada no banco de dados.")