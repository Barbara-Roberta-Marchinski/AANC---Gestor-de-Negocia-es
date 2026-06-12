import joblib
import pandas as pd
model = joblib.load('ml/modelo_turnover.pkl')
df = pd.read_csv('data/ibm_attrition.csv', nrows=1)
if 'Attrition' in df.columns:
    df = df.drop(columns=['Attrition'])
base = df.iloc[[0]].copy()
print('cols', list(df.columns))
print('dtypes', dict(df.dtypes))
print('base', base.to_dict(orient='records')[0])
print('---')
for monthly_income in [5000,7000,9000,12000]:
    test = base.copy()
    test.at[test.index[0], 'MonthlyIncome'] = monthly_income
    if 'PercentSalaryHike' in test.columns:
        test.at[test.index[0], 'PercentSalaryHike'] = 10
    print(monthly_income, float(model.predict_proba(test)[0][1]))
