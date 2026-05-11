"""Script de utilitário para criar a estrutura inicial do projeto e arquivos de configuração."""

import os

# Criar pastas
os.makedirs('data', exist_ok=True)
os.makedirs('src', exist_ok=True)
os.makedirs('docs', exist_ok=True)

# Criar .env
with open('.env', 'w') as f:
    f.write('# Arquivo para armazenar chaves de API\n')

# Criar .gitignore
with open('.gitignore', 'w') as f:
    f.write('# Ignore environment variables\n')
    f.write('.env\n')
    f.write('\n# Ignore virtual environment\n')
    f.write('env_aanc/\n')