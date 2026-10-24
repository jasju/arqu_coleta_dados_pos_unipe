# arqu_coleta_dados_pos_unipe

## Gerenciamento de Dependências com uv

Este projeto usa [uv](https://github.com/astral-sh/uv) para gerenciamento de pacotes Python. O uv foi adicionado para substituir o pip tradicional, oferecendo instalações muito mais rápidas e sincronização de ambientes mais confiável.

### Instalação do uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Sincronização do ambiente

Para instalar todas as dependências listadas no `pyproject.toml` e `uv.lock`:

```bash
uv sync
```

Isso instalará exatamente as versões específicadas e gerará um ambiente isolado em `.venv`. Para ativar o ambiente:

- **Linux/macOS**: `source .venv/bin/activate`
- **Windows PowerShell**: `.venv\Scripts\Activate.ps1`
- **Windows CMD**: `.venv\Scripts\activate.bat`

### Comandos avançados(opcionais)

```bash
# Instalar apenas dependências de desenvolvimento
uv sync --dev

# Atualizar todas as dependências para as versões mais recentes compatíveis
uv lock --upgrade-minor
uv sync

# Gerar um requirements.txt a partir do lockfile
uv pip compile uv.lock -o requirements.txt
```

## Desenvolvimento

... (outras seções do README)

# Referências

- [Documentação do uv](https://docs.astral.sh/uv/)
- [Guia de migração do pip para uv](https://astral.sh/blog/migrating-from-pip-to-uv)
