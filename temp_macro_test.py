import joblib
import pandas as pd

modelo = joblib.load('ml/modelo_turnover.pkl')
df = pd.read_csv('data/ibm_attrition.csv', nrows=1)
if 'Attrition' in df.columns:
    df = df.drop(columns=['Attrition'])
plants = {'G1': (6000, 40), 'G2': (6100, 40), 'G3': (6200, 40)}
for name, (base, age) in plants.items():
    print('---', name)
    for pct in [0, 1, 2, 5, 10, 15]:
        novo = int(round(base * (1 + pct / 100)))
        d = df.copy()
        d.at[d.index[0], 'Age'] = age
        d.at[d.index[0], 'MonthlyIncome'] = novo
        d.at[d.index[0], 'PercentSalaryHike'] = pct
        p = float(modelo.predict_proba(d)[0][1])
        print(name, pct, novo, p)
