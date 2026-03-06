import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from neo4j import GraphDatabase
from pymongo import MongoClient
from redis import Redis
from redis.exceptions import RedisError

try:
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    CASSANDRA_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:
    Cluster = None  # type: ignore[assignment]
    SimpleStatement = None  # type: ignore[assignment]
    CASSANDRA_IMPORT_ERROR = exc


@dataclass
class AppConfig:
    # Parametros da API, moeda e conexoes para cada banco NoSQL.
    api_url: str = os.getenv(
        "API_URL", "https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL"
    )
    moeda_monitorada: str = os.getenv("MOEDA_MONITORADA", "USDBRL")
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_ttl: int = int(os.getenv("REDIS_TTL", "45"))
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db: str = os.getenv("MONGO_DB", "mercado")
    mongo_collection: str = os.getenv("MONGO_COLLECTION", "cotacoes_raw")
    cassandra_hosts: List[str] = field(
        default_factory=lambda: list(
            h.strip() for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
        )
    )
    cassandra_port: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    cassandra_keyspace: str = os.getenv("CASSANDRA_KEYSPACE", "mercado")
    cassandra_table: str = os.getenv("CASSANDRA_TABLE", "historico_precos")
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "password123")
    investidores: List[str] = field(
        default_factory=lambda: list(
            p.strip() for p in os.getenv("INVESTIDORES", "Alice,Bob,Carlos").split(",")
        )
    )
    loop_interval: int = int(os.getenv("LOOP_INTERVAL", "10"))
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "10"))


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}")


def conectar_redis(cfg: AppConfig) -> Optional[Redis]:
    try:
        client = Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=True)
        client.ping()
        log("[REDIS] Conexao estabelecida.")
        return client
    except RedisError as exc:
        log(f"[REDIS] Falha de conexao: {exc}")
        return None


def conectar_mongo(cfg: AppConfig) -> Optional[MongoClient]:
    try:
        client = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        log("[MONGO] Conexao estabelecida.")
        return client
    except Exception as exc:
        log(f"[MONGO] Falha de conexao: {exc}")
        return None


def conectar_cassandra(cfg: AppConfig, tentativas: int = 8) -> Tuple[Optional[Any], Any]:
    # Segue em modo degradado quando o driver nao estiver disponivel.
    if Cluster is None:
        log(f"[CASSANDRA] Driver indisponivel no ambiente atual: {CASSANDRA_IMPORT_ERROR}")
        return None, None
    for tentativa in range(1, tentativas + 1):
        try:
            cluster = Cluster(list(cfg.cassandra_hosts), port=cfg.cassandra_port)
            session = cluster.connect()
            log(f"[CASSANDRA] Conexao estabelecida na tentativa {tentativa}.")
            return cluster, session
        except Exception as exc:
            espera = min(2 * tentativa, 15)
            log(
                f"[CASSANDRA] Tentativa {tentativa}/{tentativas} falhou: {exc}. "
                f"Novo retry em {espera}s."
            )
            time.sleep(espera)
    return None, None


def conectar_neo4j(cfg: AppConfig):
    try:
        driver = GraphDatabase.driver(
            cfg.neo4j_uri, auth=(cfg.neo4j_user, cfg.neo4j_password)
        )
        driver.verify_connectivity()
        log("[NEO4J] Conexao estabelecida.")
        return driver
    except Exception as exc:
        log(f"[NEO4J] Falha de conexao: {exc}")
        return None


def setup_cassandra(session: Any, cfg: AppConfig) -> None:
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {cfg.cassandra_keyspace}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
        """
    )
    session.set_keyspace(cfg.cassandra_keyspace)
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.cassandra_table} (
            moeda TEXT,
            data_coleta TIMESTAMP,
            valor DOUBLE,
            variacao TEXT,
            PRIMARY KEY ((moeda), data_coleta)
        ) WITH CLUSTERING ORDER BY (data_coleta DESC)
        """
    )
    log("[CASSANDRA] Keyspace e tabela garantidos.")


def setup_neo4j(driver: Any, cfg: AppConfig) -> None:
    with driver.session() as session:
        session.run("MERGE (:Moeda {codigo: $codigo})", codigo=cfg.moeda_monitorada)
        # Garante investidores e relacoes antes do loop principal.
        for investidor in cfg.investidores:
            session.run(
                """
                MERGE (i:Investidor {nome: $nome})
                MERGE (m:Moeda {codigo: $codigo})
                MERGE (i)-[:ACOMPANHA]->(m)
                """,
                nome=investidor,
                codigo=cfg.moeda_monitorada,
            )
    log("[NEO4J] Setup inicial de Investidor/Moeda concluido.")


def parse_payload_awesome(payload: Dict[str, Any], moeda_monitorada: str) -> Dict[str, Any]:
    # Normaliza retorno da API para o formato usado pelo monitor.
    if moeda_monitorada not in payload:
        raise ValueError(
            f"Moeda monitorada '{moeda_monitorada}' nao encontrada no payload da API."
        )
    item = payload[moeda_monitorada]
    return {
        "moeda": moeda_monitorada,
        "valor": float(item["bid"]),
        "variacao": item.get("pctChange") or item.get("varBid") or "0",
        "payload_bruto": payload,
    }


def obter_cotacao_cache(redis_client: Redis, chave: str) -> Optional[Dict[str, Any]]:
    dado = redis_client.get(chave)
    if not dado:
        return None
    return json.loads(dado)


def buscar_na_api(cfg: AppConfig) -> Dict[str, Any]:
    resposta = requests.get(cfg.api_url, timeout=cfg.request_timeout)
    resposta.raise_for_status()
    payload = resposta.json()
    return parse_payload_awesome(payload, cfg.moeda_monitorada)


def obter_cotacao(cfg: AppConfig, redis_client: Optional[Redis]) -> Optional[Dict[str, Any]]:
    chave = f"cotacao:{cfg.moeda_monitorada}"
    if not redis_client:
        log("[REDIS] Indisponivel. Buscando diretamente na API.")
        try:
            cotacao = buscar_na_api(cfg)
            log("[API] Cotacao obtida sem cache.")
            return cotacao
        except Exception as exc:
            log(f"[API] Erro ao buscar cotacao: {exc}")
            return None

    try:
        # Estrategia cache-aside: primeiro tenta Redis, depois API.
        cache_data = obter_cotacao_cache(redis_client, chave)
        if cache_data:
            log("[REDIS] Cache Hit! Cotacao recuperada do Redis.")
            return cache_data
        log("[REDIS] Cache Miss! Buscando na API.")
        cotacao = buscar_na_api(cfg)
        redis_client.setex(chave, cfg.redis_ttl, json.dumps(cotacao))
        log(f"[REDIS] Cache atualizado com TTL de {cfg.redis_ttl}s.")
        return cotacao
    except Exception as exc:
        log(f"[REDIS/API] Falha no fluxo de cotacao: {exc}")
        return None


def salvar_mongo(mongo_client: Optional[MongoClient], cfg: AppConfig, cotacao: Dict[str, Any]) -> None:
    if not mongo_client:
        log("[MONGO] Indisponivel. Registro nao salvo.")
        return
    try:
        # Data Lake bruto para auditoria e reprocessamento posterior.
        doc = {
            "Moeda": cotacao["moeda"],
            "Valor": cotacao["valor"],
            "Variacao": cotacao["variacao"],
            "payload_bruto": cotacao["payload_bruto"],
            "data_coleta": datetime.now(),
        }
        mongo_client[cfg.mongo_db][cfg.mongo_collection].insert_one(doc)
        log("[MONGO] Payload bruto salvo no Data Lake.")
    except Exception as exc:
        log(f"[MONGO] Erro ao salvar: {exc}")


def salvar_cassandra(session: Any, cfg: AppConfig, cotacao: Dict[str, Any]) -> None:
    if not session or SimpleStatement is None:
        log("[CASSANDRA] Indisponivel. Registro nao salvo.")
        return
    try:
        query = SimpleStatement(
            f"""
            INSERT INTO {cfg.cassandra_keyspace}.{cfg.cassandra_table}
            (moeda, data_coleta, valor, variacao)
            VALUES (%s, %s, %s, %s)
            """
        )
        session.execute(
            query,
            (cotacao["moeda"], datetime.now(), float(cotacao["valor"]), str(cotacao["variacao"])),
        )
        log(f"[CASSANDRA] Preco de {cotacao['valor']} gravado na serie temporal.")
    except Exception as exc:
        log(f"[CASSANDRA] Erro ao salvar: {exc}")


def notificar_neo4j(driver: Any, moeda: str) -> None:
    if not driver:
        log("[NEO4J] Indisponivel. Notificacao nao executada.")
        return
    try:
        with driver.session() as session:
            resultado = session.run(
                """
                MATCH (i:Investidor)-[:ACOMPANHA]->(m:Moeda {codigo: $codigo})
                RETURN i.nome AS nome
                ORDER BY nome
                """,
                codigo=moeda,
            )
            nomes = [r["nome"] for r in resultado]
            if nomes:
                log(f"[NEO4J] Notificando investidores: {', '.join(nomes)}.")
            else:
                log(f"[NEO4J] Nenhum investidor acompanha {moeda}.")
    except Exception as exc:
        log(f"[NEO4J] Erro ao consultar investidores: {exc}")


def main() -> None:
    cfg = AppConfig()
    log(f"Consultando preco de {cfg.moeda_monitorada}...")

    # Conecta aos bancos e prepara estruturas iniciais.
    redis_client = conectar_redis(cfg)
    mongo_client = conectar_mongo(cfg)
    cassandra_cluster, cassandra_session = conectar_cassandra(cfg)
    neo4j_driver = conectar_neo4j(cfg)

    if cassandra_session:
        try:
            setup_cassandra(cassandra_session, cfg)
        except Exception as exc:
            log(f"[CASSANDRA] Erro no setup inicial: {exc}")

    if neo4j_driver:
        try:
            setup_neo4j(neo4j_driver, cfg)
        except Exception as exc:
            log(f"[NEO4J] Erro no setup inicial: {exc}")

    while True:
        # Fluxo continuo: obter cotacao, persistir e notificar.
        cotacao = obter_cotacao(cfg, redis_client)
        if cotacao:
            salvar_mongo(mongo_client, cfg, cotacao)
            salvar_cassandra(cassandra_session, cfg, cotacao)
            notificar_neo4j(neo4j_driver, cotacao["moeda"])
        time.sleep(cfg.loop_interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Encerrado manualmente.")
