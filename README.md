# Plataforma de Inteligência de Mercado (NoSQL)

Projeto final da disciplina de Banco de Dados NoSQL com arquitetura de persistência poliglota:

- Redis para cache de baixa latência
- MongoDB para Data Lake (payload bruto)
- Cassandra para série temporal
- Neo4j para relacionamento investidor-moeda e alertas

## Visão Geral

O script `monitor.py` consulta periodicamente a cotação de uma moeda (por padrão `USDBRL`), aplica estratégia de cache no Redis e persiste os dados em múltiplos bancos especializados por responsabilidade.

Fluxo principal por ciclo:

1. Verifica cache no Redis (hit/miss com TTL)
2. Busca API em caso de miss
3. Salva payload bruto no MongoDB com `data_coleta`
4. Salva série temporal no Cassandra (`historico_precos`)
5. Consulta investidores no Neo4j e imprime notificações

## Estrutura do Projeto

```text
.
├── monitor.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Tecnologias

- Python 3.10+
- Docker + Docker Compose
- Redis
- MongoDB
- Cassandra
- Neo4j

Bibliotecas Python:

- `requests`
- `redis`
- `pymongo`
- `cassandra-driver`
- `neo4j`

## Pré-requisitos

Antes de iniciar, garanta que você possui:

- Docker Desktop instalado e em execução
- Python 3.10+ instalado
- Acesso à internet para consumir a API pública

## Subindo a infraestrutura

No diretório do projeto:

```bash
docker compose up -d
```

Serviços esperados:

- Redis: `localhost:6379`
- MongoDB: `localhost:27017`
- Cassandra: `localhost:9042`
- Neo4j:
  - Browser: `http://localhost:7474`
  - Bolt: `bolt://localhost:7687`
  - Credenciais padrão: `neo4j / password123`

## Ambiente Python

Crie e ative um ambiente virtual:

```bash
python -m venv .venv
```

Windows (PowerShell):

```powershell
.\.venv\Scripts\Activate.ps1
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

## Executando o monitor

Com os containers ativos e o ambiente virtual ativado:

```bash
python monitor.py
```

O script roda em loop contínuo (`while True`) e exibe logs de cada etapa da orquestração.

## Configuração por variáveis de ambiente

O script já possui valores padrão, mas você pode personalizar:

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `API_URL` | `https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL` | Endpoint da API de cotação |
| `MOEDA_MONITORADA` | `USDBRL` | Código da moeda monitorada |
| `REDIS_HOST` | `localhost` | Host do Redis |
| `REDIS_PORT` | `6379` | Porta do Redis |
| `REDIS_TTL` | `45` | TTL do cache em segundos |
| `MONGO_URI` | `mongodb://localhost:27017` | URI de conexão Mongo |
| `MONGO_DB` | `mercado` | Banco Mongo |
| `MONGO_COLLECTION` | `cotacoes_raw` | Coleção Mongo |
| `CASSANDRA_HOSTS` | `localhost` | Hosts Cassandra (separados por vírgula) |
| `CASSANDRA_PORT` | `9042` | Porta Cassandra |
| `CASSANDRA_KEYSPACE` | `mercado` | Keyspace Cassandra |
| `CASSANDRA_TABLE` | `historico_precos` | Tabela de série temporal |
| `NEO4J_URI` | `bolt://localhost:7687` | URI Bolt do Neo4j |
| `NEO4J_USER` | `neo4j` | Usuário Neo4j |
| `NEO4J_PASSWORD` | `password123` | Senha Neo4j |
| `INVESTIDORES` | `Alice,Bob,Carlos` | Lista inicial de investidores |
| `LOOP_INTERVAL` | `10` | Intervalo do loop em segundos |
| `REQUEST_TIMEOUT` | `10` | Timeout de requisição HTTP |

Exemplo (PowerShell):

```powershell
$env:MOEDA_MONITORADA="EURBRL"
$env:REDIS_TTL="60"
python monitor.py
```

## Exemplo de logs esperados

```text
2026-03-05 12:00:00 | Consultando preco de USDBRL...
2026-03-05 12:00:00 | [REDIS] Cache Miss! Buscando na API.
2026-03-05 12:00:00 | [REDIS] Cache atualizado com TTL de 45s.
2026-03-05 12:00:00 | [MONGO] Payload bruto salvo no Data Lake.
2026-03-05 12:00:00 | [CASSANDRA] Preco de 5.12 gravado na serie temporal.
2026-03-05 12:00:00 | [NEO4J] Notificando investidores: Alice, Bob, Carlos.
```

## Troubleshooting rápido

- Cassandra pode levar mais tempo para ficar pronto após `docker compose up`.
- Se o script iniciar antes de um serviço ficar disponível, ele entra em modo degradado e registra falha de conexão no log.
- Valide containers com:

```bash
docker compose ps
```

## Critérios atendidos no projeto

- Orquestração de 4 bancos NoSQL
- Cache hit/miss com TTL no Redis
- Persistência de payload bruto no MongoDB com timestamp
- Série temporal no Cassandra com modelagem por moeda e ordenação temporal
- Grafo de investidores e consulta de alertas no Neo4j