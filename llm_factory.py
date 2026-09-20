#!/usr/bin/env python3
# Copyright (c) 2026 hidemi-k
# Licensed under the MIT License.
"""
llm_factory.py — LLM 初期化・フォールバック共通モジュール（全サーバ共用）
==========================================================================
全 A2A サーバで共有する LLM 初期化ロジック。
Groq を Primary、Azure OpenAI を Fallback として自動切り替えする。

【設計方針】
  Primary   : Groq (openai/gpt-oss-120b)
                高速推論（TTFT ~100ms）
  Fallback  : Azure OpenAI (gpt-4.1-mini)
                プライベートエンドポイント・安定稼働

【フォールバック発動条件】
  - Groq への接続エラー（ConnectionError）
  - レート制限 (429 Too Many Requests)
  - タイムアウト（LLM_TIMEOUT 秒）
  - その他の API エラー

【設定（config.ini または環境変数）】
  [GROQ]
  GROQ_API_KEY = gsk_xxxx

  [AZURE]
  AZURE_OPENAI_API_KEY      = xxxx
  AZURE_OPENAI_ENDPOINT     = https://maf-llm-api.openai.azure.com/
  AZURE_OPENAI_DEPLOYMENT   = gpt-4.1-mini
  AZURE_OPENAI_API_VERSION  = 2025-01-01-preview

  環境変数:
  LLM_PROVIDER = auto | groq | azure   （デフォルト: auto）
  LLM_TIMEOUT  = 30                    （デフォルト: 30秒）

LLM_PROVIDER:
  auto  : Groq Primary → 失敗時 Azure Fallback（推奨）
  groq  : Groq のみ（フォールバックなし）
  azure : Azure のみ（フォールバックなし）

【使い方】
  # 各サーバの既存コードをこれで置き換える:

  # Before（各サーバに重複していたコード）:
  GROQ_API_KEY = _load_groq_key()
  llm = ChatOpenAI(model=DEFAULT_MODEL, ...)

  # After（llm_factory に一元化）:
  from llm_factory import build_llm, invoke_with_fallback, LLM_PROVIDER_NAME

  llm = build_llm()                        # 通常の LangChain チェーン用
  result = await invoke_with_fallback(llm, prompt)  # フォールバック付き呼び出し

【NETCONF サーバ専用 (make_client 置き換え)】:
  from llm_factory import build_autogen_client
  client = build_autogen_client()          # AutoGen OpenAIChatCompletionClient
"""

import configparser
import logging
import os
import time
from typing import Optional, Union

logger = logging.getLogger("llm_factory")

# ── 設定パス ────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.getenv(
    "SASE_CONFIG",
    os.path.join(BASE_DIR, "./config.ini"),
)

# ── Groq 設定 ────────────────────────────────────────────────────────────────────
GROQ_BASE_URL    = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
GROQ_MODEL       = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

# ── Azure OpenAI 設定 ─────────────────────────────────────────────────────────────
AZURE_DEPLOYMENT  = os.getenv("AZURE_OPENAI_DEPLOYMENT",  "gpt-4.1-mini")
AZURE_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")

# ── 動作モード ────────────────────────────────────────────────────────────────────
# auto : Groq Primary → Azure Fallback
# groq : Groq のみ
# azure: Azure のみ
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "auto").lower()

# Groq タイムアウト（秒）: これを超えたら Azure に切り替え
LLM_TIMEOUT  = float(os.getenv("LLM_TIMEOUT", "30"))


# ═══════════════════════════════════════════════════════════════════════════════
# 設定読み込み
# ═══════════════════════════════════════════════════════════════════════════════

def _load_config() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if os.path.exists(CONFIG_PATH):
        cfg.read(CONFIG_PATH, encoding="utf-8")
    return cfg


def _load_groq_key() -> str:
    """Groq API キーを環境変数 → config.ini の順で取得する。"""
    key = os.getenv("GROQ_API_KEY", "")
    if key:
        return key
    cfg = _load_config()
    if "GROQ" in cfg and "GROQ_API_KEY" in cfg["GROQ"]:
        return cfg["GROQ"]["GROQ_API_KEY"].strip()
    return ""


def _load_azure_config() -> dict:
    """
    Azure OpenAI 接続情報を環境変数 → config.ini の順で取得する。

    config.ini の記載例:
      [AZURE]
      AZURE_OPENAI_API_KEY     = xxxx
      AZURE_OPENAI_ENDPOINT    = https://maf-llm-api.openai.azure.com/
      AZURE_OPENAI_DEPLOYMENT  = gpt-4.1-mini
      AZURE_OPENAI_API_VERSION = 2025-01-01-preview
    """
    cfg = _load_config()
    az  = cfg["AZURE"] if "AZURE" in cfg else {}

    api_key  = os.getenv("AZURE_OPENAI_API_KEY",  az.get("AZURE_OPENAI_API_KEY",  ""))
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT",  az.get("AZURE_OPENAI_ENDPOINT", ""))
    deploy   = os.getenv("AZURE_OPENAI_DEPLOYMENT",
                          az.get("AZURE_OPENAI_DEPLOYMENT", AZURE_DEPLOYMENT))
    version  = os.getenv("AZURE_OPENAI_API_VERSION",
                          az.get("AZURE_OPENAI_API_VERSION", AZURE_API_VERSION))

    return {
        "api_key":    api_key.strip()  if api_key  else "",
        "endpoint":   endpoint.strip() if endpoint else "",
        "deployment": deploy.strip()   if deploy   else AZURE_DEPLOYMENT,
        "api_version": version.strip() if version  else AZURE_API_VERSION,
    }


# 起動時にキー・設定を読み込む
_GROQ_KEY   = _load_groq_key()
_AZURE_CFG  = _load_azure_config()

# 利用可能プロバイダーをログに記録
_groq_ok  = bool(_GROQ_KEY)
_azure_ok = bool(_AZURE_CFG["api_key"] and _AZURE_CFG["endpoint"])

LLM_PROVIDER_NAME: str   # 実際に使用するプロバイダー名（起動ログ用）

if LLM_PROVIDER == "azure":
    LLM_PROVIDER_NAME = "azure"
elif LLM_PROVIDER == "groq":
    LLM_PROVIDER_NAME = "groq"
else:
    # auto: 両方が利用可能なら "auto(groq→azure)"、片方だけなら "groq" or "azure"
    if _groq_ok and _azure_ok:
        LLM_PROVIDER_NAME = "auto (groq→azure fallback)"
    elif _groq_ok:
        LLM_PROVIDER_NAME = "groq (azure key なし)"
    elif _azure_ok:
        LLM_PROVIDER_NAME = "azure (groq key なし)"
    else:
        LLM_PROVIDER_NAME = "none (キー未設定)"


# ═══════════════════════════════════════════════════════════════════════════════
# LLM インスタンス構築
# ═══════════════════════════════════════════════════════════════════════════════

def _build_groq_llm(timeout: float = LLM_TIMEOUT):
    """Groq LLM インスタンスを構築する。"""
    from langchain_openai import ChatOpenAI
    if not _GROQ_KEY:
        raise RuntimeError("GROQ_API_KEY が見つかりません（config.ini [GROQ] または環境変数）")
    return ChatOpenAI(
        model        = GROQ_MODEL,
        temperature  = 0,
        api_key      = _GROQ_KEY,
        base_url     = GROQ_BASE_URL,
        timeout      = timeout,
        max_retries  = 0,    # フォールバック前にリトライしない
    )


def _build_azure_llm():
    """Azure OpenAI LLM インスタンスを構築する。"""
    from langchain_openai import AzureChatOpenAI
    az = _AZURE_CFG
    if not az["api_key"]:
        raise RuntimeError(
            "AZURE_OPENAI_API_KEY が見つかりません（config.ini [AZURE] または環境変数）"
        )
    if not az["endpoint"]:
        raise RuntimeError(
            "AZURE_OPENAI_ENDPOINT が見つかりません（config.ini [AZURE] または環境変数）"
        )
    return AzureChatOpenAI(
        azure_deployment     = az["deployment"],
        azure_endpoint       = az["endpoint"],
        api_version          = az["api_version"],
        api_key              = az["api_key"],
        ### temperature          = 0,  temperature を指定しない → モデル側のデフォルト(1)がそのまま使われる
        max_retries          = 2,
    )


def build_llm(provider: Optional[str] = None):
    """
    LLM インスタンスを構築して返す。

    LangChain チェーン（RAG / プロンプト）で使用する通常の LLM。
    フォールバック付きの呼び出しは invoke_with_fallback() を使用すること。

    Args:
        provider: "groq" | "azure" | "auto" | None（None は LLM_PROVIDER 環境変数）
    """
    p = (provider or LLM_PROVIDER).lower()

    if p == "azure":
        llm = _build_azure_llm()
        logger.info(f"LLM: Azure OpenAI ({_AZURE_CFG['deployment']}) を使用")
        return llm

    if p in ("groq", "auto"):
        try:
            llm = _build_groq_llm()
            logger.info(f"LLM: Groq ({GROQ_MODEL}) を使用")
            return llm
        except RuntimeError:
            if p == "auto" and _azure_ok:
                logger.warning("Groq キーなし → Azure OpenAI にフォールバック")
                return _build_azure_llm()
            raise

    raise ValueError(f"不明な LLM_PROVIDER: {p!r}  (groq / azure / auto)")


# ═══════════════════════════════════════════════════════════════════════════════
# フォールバック付き呼び出し
# ═══════════════════════════════════════════════════════════════════════════════

# フォールバックが発動するエラーキーワード
_FALLBACK_TRIGGERS = (
    "rate_limit",
    "rate limit",
    "429",
    "timeout",
    "connection",
    "serviceunavailable",
    "service_unavailable",
    "503",
    "overloaded",
)


def _is_fallback_error(e: Exception) -> bool:
    """フォールバックを発動すべきエラーかどうかを判定する。"""
    msg = str(e).lower()
    return any(kw in msg for kw in _FALLBACK_TRIGGERS)


def invoke_with_fallback(llm, prompt: str) -> str:
    """
    Groq Primary で LLM を呼び出し、失敗時に Azure Fallback に切り替える。

    LangChain チェーン外で LLM を直接呼び出す場合（task_decompose の
    classify_query 等）に使用する。

    Args:
        llm   : build_llm() で構築した LLM インスタンス（Primary）
        prompt: プロンプト文字列

    Returns:
        LLM の応答テキスト
    """
    # Groq / Azure 固定モードはフォールバックなし
    if LLM_PROVIDER in ("groq", "azure") or not _azure_ok:
        response = llm.invoke(prompt)
        return response.content if hasattr(response, "content") else str(response)

    # auto モード: Groq → Azure フォールバック
    try:
        t0       = time.time()
        response = llm.invoke(prompt)
        elapsed  = time.time() - t0
        logger.debug(f"Groq 応答: {elapsed:.2f}s")
        return response.content if hasattr(response, "content") else str(response)

    except Exception as e:
        if not _is_fallback_error(e):
            raise   # 構文エラー等はフォールバックしない

        logger.warning(
            f"Groq エラー ({type(e).__name__}: {str(e)[:80]}) "
            f"→ Azure OpenAI にフォールバック"
        )
        try:
            azure_llm = _build_azure_llm()
            response  = azure_llm.invoke(prompt)
            logger.info("Azure OpenAI フォールバック成功")
            return response.content if hasattr(response, "content") else str(response)
        except Exception as e2:
            logger.error(f"Azure フォールバックも失敗: {e2}")
            raise RuntimeError(
                f"Groq ({e}) / Azure ({e2}) 両方失敗"
            ) from e2


async def ainvoke_with_fallback(llm, prompt: str) -> str:
    """
    invoke_with_fallback の非同期版。
    async def execute() 内で使用する場合はこちらを使う。
    """
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, lambda: invoke_with_fallback(llm, prompt)
    )


# ═══════════════════════════════════════════════════════════════════════════════
# LangChain チェーン用フォールバック対応 LLM（with_fallbacks）
# ═══════════════════════════════════════════════════════════════════════════════

def build_llm_with_fallback():
    """
    LangChain の .with_fallbacks() を使ったフォールバック対応 LLM を返す。
    RAG チェーン（chain = prompt | llm | parser）で直接使用できる。

    auto モードかつ Azure が利用可能な場合のみフォールバックを設定する。
    それ以外は通常の build_llm() と同じ動作。

    使用例:
        llm = build_llm_with_fallback()
        chain = prompt | llm | StrOutputParser()
        result = chain.invoke({"question": "..."})
    """
    primary = build_llm()

    if LLM_PROVIDER not in ("auto",) or not _azure_ok:
        return primary

    try:
        azure_llm = _build_azure_llm()
        # LangChain の with_fallbacks: primary が例外を出したら azure_llm を試みる
        return primary.with_fallbacks(
            [azure_llm],
            exceptions_to_handle=(Exception,),
        )
    except Exception as e:
        logger.warning(f"Azure LLM 構築失敗、フォールバックなしで続行: {e}")
        return primary


# ═══════════════════════════════════════════════════════════════════════════════
# NETCONF サーバ専用: AutoGen OpenAIChatCompletionClient
# ═══════════════════════════════════════════════════════════════════════════════

def build_autogen_client():
    """
    arista_netconf_rag_a2a_server.py の make_client() を置き換える。
    agent_framework_openai.OpenAIChatCompletionClient を返す。

    元のサーバは agent_framework_openai（autogen_ext とは別ライブラリ）を
    使用しているため、そのライブラリから import する。

    auto モード: Groq Primary → Azure Fallback の順で試みる。
    """
    # agent_framework_openai は autogen_ext とは別の独自ラッパー
    # （arista_netconf_rag_a2a_server.py の元の実装に合わせる）
    from agent_framework_openai import OpenAIChatCompletionClient

    p = LLM_PROVIDER.lower()

    if p == "azure":
        return _build_agentfw_azure(OpenAIChatCompletionClient)

    if p in ("groq", "auto"):
        if _GROQ_KEY:
            try:
                client = OpenAIChatCompletionClient(
                    model    = GROQ_MODEL,
                    api_key  = _GROQ_KEY,
                    base_url = GROQ_BASE_URL,
                )
                logger.info(f"AutoGen Client: Groq ({GROQ_MODEL})")
                return client
            except Exception as e:
                if p == "groq":
                    raise
                logger.warning(f"AutoGen Groq 初期化失敗 → Azure: {e}")
        if p == "auto" and _azure_ok:
            return _build_agentfw_azure(OpenAIChatCompletionClient)
        raise RuntimeError("GROQ_API_KEY が見つかりません")

    raise ValueError(f"不明な LLM_PROVIDER: {p!r}")


def _build_agentfw_azure(ClientClass):
    """
    Azure OpenAI の agent_framework_openai クライアントを構築する。
    ClientClass は agent_framework_openai.OpenAIChatCompletionClient。

    agent_framework_openai が Azure エンドポイントをどう受け取るかは
    ライブラリの実装に依存するため、Groq 互換形式（base_url + api_key）で
    渡す方式をデフォルトとする。
    """
    az = _AZURE_CFG
    # Azure OpenAI の REST エンドポイントを OpenAI 互換形式で渡す
    # 例: https://maf-llm-api.openai.azure.com/openai/deployments/gpt-4.1-mini/
    base_url = (
        f"{az['endpoint'].rstrip('/')}"
        f"/openai/deployments/{az['deployment']}/"
    )
    try:
        client = ClientClass(
            model    = az["deployment"],
            api_key  = az["api_key"],
            base_url = base_url,
            # Azure API バージョンをクエリパラメータとして付加する必要がある場合は
            # ライブラリの仕様を確認してください
        )
        logger.info(f"AutoGen Client: Azure OpenAI ({az['deployment']}) via agent_framework_openai")
        return client
    except Exception as e:
        logger.error(f"Azure agent_framework_openai クライアント構築失敗: {e}")
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 起動時サマリーログ
# ═══════════════════════════════════════════════════════════════════════════════

def log_llm_config(server_name: str = "") -> None:
    """サーバ起動ログに LLM 設定を出力する。"""
    prefix = f"[{server_name}] " if server_name else ""
    logger.info(f"{prefix}LLM Provider  : {LLM_PROVIDER_NAME}")
    logger.info(f"{prefix}Groq model    : {GROQ_MODEL}  (key={'✅' if _groq_ok else '❌ 未設定'})")
    logger.info(f"{prefix}Azure deploy  : {_AZURE_CFG['deployment']}  "
                f"(key={'✅' if _azure_ok else '❌ 未設定'})")
    if LLM_PROVIDER == "auto" and _groq_ok and not _azure_ok:
        logger.warning(
            f"{prefix}⚠️  Azure キー未設定 — Groq 障害時のフォールバックが利用できません"
        )
