import joblib
import pandas as pd

model = joblib.load('ml/modelo_turnover.pkl')
template = pd.read_csv('data/ibm_attrition.csv', nrows=1)
if 'Attrition' in template.columns:
    template = template.drop(columns=['Attrition'])

plants = {
    'G1': {'Salário Base': 6000, 'Age': 35, 'BusinessTravel': 'Travel_Rarely', 'Department': 'Sales', 'Gender': 'Female', 'JobRole': 'Sales Executive', 'MaritalStatus': 'Single', 'OverTime': 'Yes'},
    'G2': {'Salário Base': 7000, 'Age': 45, 'BusinessTravel': 'Travel_Frequently', 'Department': 'Research & Development', 'Gender': 'Male', 'JobRole': 'Laboratory Technician', 'MaritalStatus': 'Married', 'OverTime': 'No'},
    'G3': {'Salário Base': 8500, 'Age': 50, 'BusinessTravel': 'Non-Travel', 'Department': 'Human Resources', 'Gender': 'Female', 'JobRole': 'Human Resources', 'MaritalStatus': 'Divorced', 'OverTime': 'No'},
}

for key, plant in plants.items():
    print(f'=== {key} ===')
    for pct in [0, 2, 5, 8, 10, 12, 15]:
        df = template.copy()
        df.at[df.index[0], 'MonthlyIncome'] = int(round(plant['Salário Base'] * (1 + pct / 100)))
        df.at[df.index[0], 'Age'] = plant['Age']
        for feat in ['BusinessTravel', 'Department', 'Gender', 'JobRole', 'MaritalStatus', 'OverTime']:
            df.at[df.index[0], feat] = plant[feat]
        if 'PercentSalaryHike' in df.columns:
            df.at[df.index[0], 'PercentSalaryHike'] = pct
        prob = float(model.predict_proba(df)[0][1])
        print(f'Pct {pct:>2}% -> {prob:.6f}')
    print()