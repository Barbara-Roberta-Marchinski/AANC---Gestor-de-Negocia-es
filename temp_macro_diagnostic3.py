import joblib
import pandas as pd

model = joblib.load('ml/modelo_turnover.pkl')

template = pd.read_csv('data/ibm_attrition.csv', nrows=1)
if 'Attrition' in template.columns:
    template = template.drop(columns=['Attrition'])

plant = {
    'Salário Base': 6000,
    'Age': 35,
    'BusinessTravel': 'Travel_Rarely',
    'Department': 'Sales',
    'Gender': 'Female',
    'JobRole': 'Sales Executive',
    'MaritalStatus': 'Single',
    'OverTime': 'Yes'
}

base_rate = int(template.at[template.index[0], 'MonthlyRate'])
print('Base MonthlyRate', base_rate)

for pct in [0, 2, 5, 8, 10, 12, 15]:
    df = template.copy()
    novo = int(round(plant['Salário Base'] * (1 + pct / 100)))
    df.at[df.index[0], 'MonthlyIncome'] = novo
    df.at[df.index[0], 'MonthlyRate'] = int(round(base_rate * (1 + pct / 100)))
    df.at[df.index[0], 'Age'] = plant['Age']
    for feat in ['BusinessTravel', 'Department', 'Gender', 'JobRole', 'MaritalStatus', 'OverTime']:
        df.at[df.index[0], feat] = plant[feat]
    if 'PercentSalaryHike' in df.columns:
        df.at[df.index[0], 'PercentSalaryHike'] = pct
    print(pct, float(model.predict_proba(df)[0][1]))
