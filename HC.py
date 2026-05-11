import pandas as pd
import numpy as np

# 1. TABELA DE PREMISSAS (Valores por Planta)
plantas = [f'G{i}' for i in range(1, 9)]

# Valores variados para simular diferentes realidades de mercado
valores_combustivel = [600, 550, 300, 450, 400, 500, 350, 480]
valores_plr_alvo = [1410, 1210, 900, 300, 290, 450, 1000, 700] # Valor mensal alvo

premissas_data = {
    'planta': plantas,
    'ajuda_combustivel_planta': valores_combustivel,
    'plr_alvo_planta': valores_plr_alvo,
    'rat_fap': [0.02, 0.03, 0.01, 0.04, 0.02, 0.05, 0.01, 0.03],
    'perc_he_medio': [0.15, 0.10, 0.22, 0.08, 0.12, 0.18, 0.20, 0.14],
    'fgts': [0.08] * 8,
    'provisao_ferias': [0.1111] * 8,
    'provisao_13': [0.0833] * 8,
    'inss_patronal': [0.20] * 8,
    'terceiros': [0.06] * 8
}

df_premissas = pd.DataFrame(premissas_data)

# 2. TABELA DE HEADCOUNT (200 funcionários)
np.random.seed(42)
n_funcionarios = 200
cargos = ['Operação', 'Analista', 'Especialista', 'Supervisor']
prob_cargos = [0.70, 0.15, 0.10, 0.05]

data_h = {
    'id_funcionario': range(1000, 1000 + n_funcionarios),
    'planta': np.random.choice(plantas, n_funcionarios),
    'subgrupo_cargos': np.random.choice(cargos, n_funcionarios, p=prob_cargos),
}

df_headcount = pd.DataFrame(data_h)

def atribuir_valores(row):
    # Salário Base
    base_salarial = {'Operação': 2500, 'Analista': 5500, 'Especialista': 11000, 'Supervisor': 9000}
    multiplicador = 1.2 if row['planta'] == 'G1' else 1.0
    salario = base_salarial[row['subgrupo_cargos']] * multiplicador * np.random.uniform(0.9, 1.1)
    
    # VA e Combustível
    va = 1050 if row['planta'] == 'G1' else 700
    combustivel = df_premissas.loc[df_premissas['planta'] == row['planta'], 'ajuda_combustivel_planta'].values[0]
    
    # PLR (Regra: Operação e Especialista são os cargos que mais discutem PLR em mesa)
    plr_elegivel = True if row['subgrupo_cargos'] in ['Operação', 'Analista'] else False
    plr_valor = df_premissas.loc[df_premissas['planta'] == row['planta'], 'plr_alvo_planta'].values[0] if plr_elegivel else 0
    
    return pd.Series([round(salario, 2), va, combustivel, plr_elegivel, plr_valor])

df_headcount[['salario_atual', 'valor_va_atual', 'ajuda_combustivel_atual', 'plr_elegivel', 'plr_alvo_atual']] = \
    df_headcount.apply(atribuir_valores, axis=1)

# Salvando os CSVs
df_headcount.to_csv('headcount.csv', index=False, encoding='utf-8-sig')
df_premissas.to_csv('premissas_plantas.csv', index=False, encoding='utf-8-sig')

print("✅ Bases geradas: Incluído PLR variável por planta e elegibilidade por cargo.")