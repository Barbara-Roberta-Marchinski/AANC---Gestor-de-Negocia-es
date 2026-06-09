import joblib
import pandas as pd

modelo = joblib.load('ml/modelo_turnover.pkl')
df = pd.read_csv('data/ibm_attrition.csv', nrows=1)
if 'Attrition' in df.columns:
    df = df.drop(columns=['Attrition'])

plants = {
    'G1': {'Age': 35, 'MonthlyIncome': 6000, 'PercentSalaryHike': 0, 'OverTime': 'Yes', 'Department': 'Sales', 'Gender': 'Female', 'JobRole': 'Sales Executive', 'MaritalStatus': 'Single'},
    'G2': {'Age': 45, 'MonthlyIncome': 7000, 'PercentSalaryHike': 0, 'OverTime': 'No', 'Department': 'Research & Development', 'Gender': 'Male', 'JobRole': 'Laboratory Technician', 'MaritalStatus': 'Married'},
    'G3': {'Age': 55, 'MonthlyIncome': 8500, 'PercentSalaryHike': 0, 'OverTime': 'No', 'Department': 'Human Resources', 'Gender': 'Female', 'JobRole': 'Human Resources', 'MaritalStatus': 'Divorced'},
}
for name, values in plants.items():
    print('---', name)
    for pct in [0, 1, 2, 5, 10, 15]:
        d = df.copy()
        for k, v in values.items():
            d.at[d.index[0], k] = v
        novo = int(round(values['MonthlyIncome'] * (1 + pct / 100)))
        d.at[d.index[0], 'MonthlyIncome'] = novo
        d.at[d.index[0], 'PercentSalaryHike'] = pct
        p = float(modelo.predict_proba(d)[0][1])
        print(name, pct, novo, p)
