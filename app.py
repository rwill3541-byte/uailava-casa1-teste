import os
import time
import asyncio
import threading
from datetime import datetime, timezone

import aiohttp
from flask import Flask, jsonify, request, render_template_string

import midealocal.cloud as midea_cloud_module
from midealocal.cloud import get_midea_cloud
from midealocal.exceptions import MideaCloudError
from midealocal.devices.db.message import MessageQuery, MessageDBResponse, MessageDBBase
from midealocal.message import MessageType, ListTypes


app = Flask(__name__)

# Faz 3123/3176 virarem excecao, como no monitor principal.
midea_cloud_module.RAISE_FOR_ERROR_CODES = frozenset(
    set(midea_cloud_module.RAISE_FOR_ERROR_CODES) | {3123, 3176}
)

# =========================================================
# CONFIGURACAO - SOMENTE CASA 1
# =========================================================
# Recomendado: usar uma conta SmartHome separada, com somente Casa 1
# compartilhada, para nao disputar sessao com o Render principal.
MIDEA_ACCOUNT = os.environ.get(
    "MIDEA_ACCOUNT_TESTE",
    os.environ.get("MIDEA_ACCOUNT", ""),
).strip()

MIDEA_PASSWORD = os.environ.get(
    "MIDEA_PASSWORD_TESTE",
    os.environ.get("MIDEA_PASSWORD", ""),
).strip()

MIDEA_CLOUD = os.environ.get("MIDEA_CLOUD", "SmartHome").strip()
CASA1_DEVICE_ID = int(os.environ.get("CASA1_DEVICE_ID", "151732605699850"))

# Valor de teste. Hoje a maquina usa 10 e a centrifugacao comeca com
# aproximadamente 12 min restantes. Testaremos 13 para tentar iniciar
# perto de 15 min restantes. Isso precisa ser validado em um ciclo real.
ALVO_CENTRIFUGACAO = int(os.environ.get("CASA1_DEHYDRATION_TIME", "13"))

# Opcional: se definido, protege as rotas de leitura/aplicacao.
TESTE_TOKEN = os.environ.get("TESTE_TOKEN", "").strip()

# Evita cliques repetidos / rajadas de login na nuvem Midea.
_lock_acao = threading.Lock()
_ultima_acao_ts = 0.0
INTERVALO_MINIMO_ACAO = 12.0


def agora_iso():
    return datetime.now(timezone.utc).isoformat()


def autorizado():
    if not TESTE_TOKEN:
        return True
    token = request.args.get("token", "") or request.headers.get("X-Teste-Token", "")
    return token == TESTE_TOKEN


def validar_config():
    faltando = []
    if not MIDEA_ACCOUNT:
        faltando.append("MIDEA_ACCOUNT_TESTE (ou MIDEA_ACCOUNT)")
    if not MIDEA_PASSWORD:
        faltando.append("MIDEA_PASSWORD_TESTE (ou MIDEA_PASSWORD)")
    return faltando


def bytes_para_decimal(dados):
    return ",".join(str(v - 256 if v >= 128 else v) for v in dados)


def decimal_para_bytes(texto):
    return bytearray(int(v) & 0xFF for v in texto.split(","))


class MessageAjustarSomenteCentrifugacao(MessageDBBase):
    """SET DB alterando somente dehydration_time.

    No protocolo usado pela Midea DB:
      body[9] -> dehydration_time

    Todos os outros bytes permanecem 0xFF = nao alterar.
    Portanto este comando NAO muda START, POWER, programa, temperatura,
    nivel de agua, tempo de lavagem ou RPM.
    """

    def __init__(self, dehydration_time, protocol_version=3):
        super().__init__(
            protocol_version=protocol_version,
            message_type=MessageType.set,
            body_type=ListTypes.X02,
        )
        self.dehydration_time = int(dehydration_time)

    @property
    def _body(self):
        corpo = bytearray([0xFF] * 21)
        corpo[9] = self.dehydration_time & 0xFF
        return corpo


async def login_midea(session):
    cloud = get_midea_cloud(
        MIDEA_CLOUD,
        session,
        MIDEA_ACCOUNT,
        MIDEA_PASSWORD,
    )
    ok = await cloud.login()
    if not ok:
        raise RuntimeError("Login SmartHome recusado")
    return cloud


async def consultar_estado_midea(cloud, device_id):
    try:
        security = cloud._security
        consulta = MessageQuery(3).serialize()
        order = security.aes_encrypt(
            bytes_para_decimal(consulta).encode(),
            security._aes_key,
            security._aes_iv,
        ).hex().upper()

        dados = cloud._make_general_data()
        dados.pop("platformId", None)
        dados.update({
            "applianceCode": str(device_id),
            "order": order,
            "timestamp": "true",
            "isFull": "false",
        })

        resposta = await cloud._api_request(
            endpoint="/v1/appliance/transparent/send",
            data=dados,
            raise_for_error=True,
        )

        if not resposta or not resposta.get("reply"):
            return {"ok": False, "tipo": "sem_reply", "erro": "Midea sem reply"}

        texto = security.aes_decrypt(
            resposta["reply"],
            security._aes_key,
            security._aes_iv,
        )
        estado = MessageDBResponse(decimal_para_bytes(texto))

        return {
            "ok": True,
            "device_id": str(device_id),
            "power": getattr(estado, "power", None),
            "start": getattr(estado, "start", None),
            "status": getattr(estado, "status", None),
            "progress": getattr(estado, "progress", None),
            "time_remaining": getattr(estado, "time_remaining", None),
            "program": getattr(estado, "program", None),
            "temperature": getattr(estado, "temperature", None),
            "dehydration_speed": getattr(estado, "dehydration_speed", None),
            "wash_time": getattr(estado, "wash_time", None),
            "wash_time_value": getattr(estado, "wash_time_value", None),
            "dehydration_time": getattr(estado, "dehydration_time", None),
            "dehydration_time_value": getattr(estado, "dehydration_time_value", None),
            "washing_data": list(getattr(estado, "washing_data", bytearray([])) or []),
        }

    except MideaCloudError as erro:
        return {
            "ok": False,
            "tipo": "erro_nuvem",
            "codigo": getattr(erro, "code", None),
            "erro": str(erro),
        }
    except Exception as erro:
        return {"ok": False, "tipo": "erro", "erro": str(erro)}


async def enviar_centrifugacao(cloud, device_id, valor):
    security = cloud._security
    mensagem = MessageAjustarSomenteCentrifugacao(valor, 3)
    serializado = mensagem.serialize()

    order = security.aes_encrypt(
        bytes_para_decimal(serializado).encode(),
        security._aes_key,
        security._aes_iv,
    ).hex().upper()

    dados = cloud._make_general_data()
    dados.pop("platformId", None)
    dados.update({
        "applianceCode": str(device_id),
        "order": order,
        "timestamp": "true",
        "isFull": "false",
    })

    resposta = await cloud._api_request(
        endpoint="/v1/appliance/transparent/send",
        data=dados,
        raise_for_error=True,
    )

    return {
        "ok": bool(resposta),
        "reply": bool(resposta.get("reply")) if resposta else False,
    }


async def ler_casa1_async():
    async with aiohttp.ClientSession() as session:
        cloud = await login_midea(session)
        return await consultar_estado_midea(cloud, CASA1_DEVICE_ID)


async def aplicar_async():
    async with aiohttp.ClientSession() as session:
        cloud = await login_midea(session)
        antes = await consultar_estado_midea(cloud, CASA1_DEVICE_ID)

        if not antes.get("ok"):
            return {"ok": False, "acao": "falha_leitura_antes", "antes": antes}

        # Seguranca: so mexe com a maquina ligada e parada.
        if antes.get("power") is not True:
            return {
                "ok": False,
                "acao": "ligue_casa1_primeiro",
                "mensagem": "Ligue a Casa 1, deixe parada e tente novamente.",
                "antes": antes,
            }

        if antes.get("start") is True or antes.get("status") in (2, 3):
            return {
                "ok": False,
                "acao": "abortado_maquina_em_ciclo",
                "mensagem": "A Casa 1 esta em ciclo/pausada. Nao foi enviado nenhum ajuste.",
                "antes": antes,
            }

        # Para este teste, exigimos o Favorito (254) para nao editar outro programa.
        if antes.get("program") != 254:
            return {
                "ok": False,
                "acao": "selecione_favorito",
                "mensagem": "Selecione o Favorito na Casa 1, sem iniciar, e tente novamente.",
                "antes": antes,
            }

        atual = antes.get("dehydration_time_value")
        if atual == ALVO_CENTRIFUGACAO:
            return {
                "ok": True,
                "acao": "ja_estava_no_alvo",
                "alvo": ALVO_CENTRIFUGACAO,
                "antes": antes,
                "depois": antes,
            }

        comando = await enviar_centrifugacao(
            cloud,
            CASA1_DEVICE_ID,
            ALVO_CENTRIFUGACAO,
        )

        if not comando.get("ok"):
            return {
                "ok": False,
                "acao": "falha_comando",
                "antes": antes,
                "comando": comando,
            }

        await asyncio.sleep(2.0)
        depois = await consultar_estado_midea(cloud, CASA1_DEVICE_ID)

        confirmado = (
            depois.get("ok") is True
            and depois.get("dehydration_time_value") == ALVO_CENTRIFUGACAO
        )

        return {
            "ok": confirmado,
            "acao": "ajuste_confirmado" if confirmado else "comando_enviado_sem_confirmacao",
            "alvo_dehydration_time": ALVO_CENTRIFUGACAO,
            "objetivo_pratico": "centrifugacao iniciar perto de 15 min restantes",
            "observacao": "O inicio real precisa ser validado em um ciclo completo.",
            "antes": antes,
            "comando": comando,
            "depois": depois,
            "executado_em": agora_iso(),
        }


def executar_com_guard(coro_factory):
    global _ultima_acao_ts

    with _lock_acao:
        agora = time.time()
        faltam = INTERVALO_MINIMO_ACAO - (agora - _ultima_acao_ts)
        if _ultima_acao_ts and faltam > 0:
            return None, max(1, int(faltam))
        _ultima_acao_ts = agora

    return asyncio.run(coro_factory()), 0


PAGINA = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Casa 1 - Teste centrifugacao</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 760px; margin: 36px auto; padding: 0 16px; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 18px; margin-bottom: 16px; }
    .ok { color: #087a2f; font-weight: bold; }
    .warn { color: #9a5b00; font-weight: bold; }
    a.botao { display:inline-block; padding:12px 16px; border-radius:9px; text-decoration:none; background:#111; color:#fff; margin:4px 4px 4px 0; }
    code { background:#f3f3f3; padding:2px 5px; border-radius:4px; }
  </style>
</head>
<body>
  <h2>Uai Lava - Casa 1</h2>
  <div class="card">
    <p><b>Servico separado de teste.</b> Nao monitora lojas, nao controla ESP e nao executa nada sozinho.</p>
    <p>Device Casa 1: <code>{{ device_id }}</code></p>
    <p>Alvo interno de centrifugacao: <b>{{ alvo }} min</b>.</p>
    <p>Objetivo do teste: a fase de centrifugacao comecar perto de <b>15 min restantes</b>.</p>
    <p class="warn">Antes de aplicar: ligue a Casa 1, selecione Favorito e deixe parada.</p>
    <a class="botao" href="/status{% if token %}?token={{ token }}{% endif %}">Ler Casa 1</a>
    <a class="botao" href="/aplicar?confirmar=SIM{% if token %}&token={{ token }}{% endif %}">Aplicar teste</a>
  </div>
</body>
</html>
"""


@app.get("/")
def home():
    return render_template_string(
        PAGINA,
        device_id=CASA1_DEVICE_ID,
        alvo=ALVO_CENTRIFUGACAO,
        token=request.args.get("token", "") if TESTE_TOKEN else "",
    )


@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "servico": "casa1-teste-centrifugacao",
        "device_id": str(CASA1_DEVICE_ID),
        "alvo_dehydration_time": ALVO_CENTRIFUGACAO,
        "background_monitor": False,
    })


@app.get("/status")
def status():
    if not autorizado():
        return jsonify({"ok": False, "erro": "nao autorizado"}), 401

    faltando = validar_config()
    if faltando:
        return jsonify({"ok": False, "erro": "variaveis ausentes", "faltando": faltando}), 500

    resultado, espera = executar_com_guard(ler_casa1_async)
    if resultado is None:
        return jsonify({
            "ok": False,
            "erro": "aguarde antes de consultar novamente",
            "aguarde_segundos": espera,
        }), 429

    return jsonify({
        "ok": bool(resultado.get("ok")),
        "servico": "casa1-teste-centrifugacao",
        "alvo_dehydration_time": ALVO_CENTRIFUGACAO,
        "estado": resultado,
    })


@app.get("/aplicar")
def aplicar():
    if not autorizado():
        return jsonify({"ok": False, "erro": "nao autorizado"}), 401

    if request.args.get("confirmar", "").strip().upper() != "SIM":
        return jsonify({
            "ok": False,
            "erro": "confirmacao necessaria",
            "como_aplicar": "/aplicar?confirmar=SIM",
        }), 400

    faltando = validar_config()
    if faltando:
        return jsonify({"ok": False, "erro": "variaveis ausentes", "faltando": faltando}), 500

    if not (1 <= ALVO_CENTRIFUGACAO <= 254):
        return jsonify({"ok": False, "erro": "CASA1_DEHYDRATION_TIME invalido"}), 500

    resultado, espera = executar_com_guard(aplicar_async)
    if resultado is None:
        return jsonify({
            "ok": False,
            "erro": "aguarde antes de tentar novamente",
            "aguarde_segundos": espera,
        }), 429

    codigo = 200 if resultado.get("ok") else 409
    return jsonify(resultado), codigo


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
