from src.database_manager import DataManager

dm = DataManager()
dm.inicializar_tabelas()
for planta in ['G1', 'G2', 'G3', 'G4', 'G5']:
    try:
        resultado = dm.simular_cenario_completo(planta_id=planta, pct_salario=5.0, pct_va=0.0, pct_plr=10.0, pct_he_adicional=0.0)
        print('PLANTA', planta)
        print(resultado)
    except Exception as e:
        print('ERRO', planta, e)
