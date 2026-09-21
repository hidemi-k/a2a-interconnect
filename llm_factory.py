#!/usr/bin/env python3
# Copyright (c) 2026 hidemi-k
# Licensed under the MIT License.
"""
llm_factory.py — LLM 初期化・フォールバック共通モジュール（全サーバ共用）
==========================================================================
全 A2A サーバで共有する LLM 初期化ロジック。
Groq を Primary、Gemini API を Fallback として自動切り替えする。
Azure OpenAI は自動フォールバック対象からは外し、明示指定時のみ使用可能な
オプションとして残す（LLM_PROVIDER=azure を指定した場合のみ）。

【設計方針】
  Primary   : Groq (openai/gpt-oss-120b)
                高速推論（TTFT ~100ms）・開発環境向け
  Fallback  : Gemini API (gemini-3.5-flash)
  (オプション) Azure OpenAI (gpt-4.1-mini)
                自動フォールバックからは除外。LLM_PROVIDER=azure を
                明示指定した場合のみ単独で使用可能。

【フォールバック優先順位】
  auto モード: Groq → Gemini API
  gemini    : Gemini のみ
  groq      : Groq のみ
  azure     : Azure のみ（自動フォールバックには含まれない）

【フォールバック発動条件】
  - 接続エラー（ConnectionError）
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

  [GEMINI]
  GOOGLE_CLOUD_PROJECT  = your-gcp-project-id
  GOOGLE_CLOUD_LOCATION = global
  GEMINI_MODEL          = gemini-3.5-flash

  環境変数:
  LLM_PROVIDER          = auto | gemini | groq | azure（デフォルト: auto）
  LLM_TIMEOUT           = 30 （デフォルト: 30秒）
  GOOGLE_CLOUD_PROJECT  = GCP プロジェクト ID（ADC 認証時は不要な場合あり）
  GOOGLE_CLOUD_LOCATION = Gemini API リージョン（デフォルト: global）
  GEMINI_MODEL          = 使用モデル（デフォルト: gemini-3.5-flash）

LLM_PROVIDER:
  auto   : Groq → Gemini の順でフォールバック（推奨）
  gemini : Gemini API のみ（GCP VM 推奨）
  groq   : Groq のみ（フォールバックなし）
  azure  : Azure のみ（自動フォールバックには含まれない、明示指定専用）

【NETCONF サーバ専用 (make_client 置き換え)】:
  from llm_factory import build_autogen_client
  client = build_autogen_client()          # AutoGen OpenAIChatCompletionClient
  # 注意: こちらは Groq → Azure の構成のまま変更していない。
  # agent_framework_openai の Client は OpenAI 互換 API 専用のため。
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

# ── Gemini API 設定 ────────────────────────────────────────────────────────
GEMINI_MODEL    = os.getenv("GEMINI_MODEL",          "gemini-3.5-flash")
GEMINI_PROJECT  = os.getenv("GOOGLE_CLOUD_PROJECT",  "")
GEMINI_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "global")

# ── 動作モード ────────────────────────────────────────────────────────────────────
# auto   : Gemini Primary → Groq Secondary → Azure Fallback
# gemini : Gemini のみ
# groq   : Groq のみ
# azure  : Azure のみ
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


def _load_gemini_config() -> dict:
    """
    Gemini API 接続情報を環境変数 → config.ini の順で取得する。

    GCP VM 上では ADC（Application Default Credentials）が自動的に使われるため
    明示的なキー設定は不要。サービスアカウントに roles/aiplatform.user を付与。

    config.ini の記載例:
      [GEMINI]
      GOOGLE_CLOUD_PROJECT  = your-gcp-project-id
      GOOGLE_CLOUD_LOCATION = global
      GEMINI_MODEL          = gemini-3.5-flash
    """
    cfg = _load_config()
    gm  = cfg["GEMINI"] if "GEMINI" in cfg else {}

    project  = os.getenv("GOOGLE_CLOUD_PROJECT",  gm.get("GOOGLE_CLOUD_PROJECT",  GEMINI_PROJECT))
    location = os.getenv("GOOGLE_CLOUD_LOCATION", gm.get("GOOGLE_CLOUD_LOCATION", GEMINI_LOCATION))
    model    = os.getenv("GEMINI_MODEL",           gm.get("GEMINI_MODEL",          GEMINI_MODEL))

    return {
        "project":  project.strip()  if project  else "",
        "location": location.strip() if location else "global",
        "model":    model.strip()    if model    else "gemini-3.5-flash",
    }


# 起動時にキー・設定を読み込む
_GROQ_KEY   = _load_groq_key()
_AZURE_CFG  = _load_azure_config()
_GEMINI_CFG = _load_gemini_config()

# 利用可能プロバイダーをログに記録
_groq_ok   = bool(_GROQ_KEY)
_azure_ok  = bool(_AZURE_CFG["api_key"] and _AZURE_CFG["endpoint"])

def _check_gemini_available() -> bool:
    """
    Gemini が利用可能かチェックする。
    """
    try:
        from langchain_google_vertexai import ChatVertexAI  # noqa: F401
        return True
    except ImportError:
        return False

_gemini_ok = _check_gemini_available()

LLM_PROVIDER_NAME: str   # 実際に使用するプロバイダー名（起動ログ用）

if LLM_PROVIDER == "gemini":
    LLM_PROVIDER_NAME = f"gemini ({_GEMINI_CFG['model']})"
elif LLM_PROVIDER == "azure":
    LLM_PROVIDER_NAME = "azure"
elif LLM_PROVIDER == "groq":
    LLM_PROVIDER_NAME = "groq"
else:
    # auto: Groq → Gemini の順
    parts = []
    if _groq_ok:
        parts.append("groq")
    if _gemini_ok:
        parts.append(f"gemini({_GEMINI_CFG['model']})")
    LLM_PROVIDER_NAME = "auto (" + "→".join(parts) + ")" if parts else "none (設定なし)"


# ═══════════════════════════════════════════════════════════════════════════════
# LLM インスタンス構築
# ═══════════════════════════════════════════════════════════════════════════════

def _build_gemini_llm():
    """
    Gemini API LLM インスタンスを構築する。

    GCP VM 上では ADC（Application Default Credentials）で自動認証される。
    ローカル開発時は以下のいずれかを実行:
      $ gcloud auth application-default login
      $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

    必要パッケージ:
      pip install "langchain-google-vertexai>=3.2.4"

    【temperature に関する注記】
      Google は Gemini 3.x 系について、既定 temperature を 0.7 ではなく
      1.0 とすることを推奨しており、0 系の低temperatureでは推論品質の
      低下や無限ループが起きうるとしている。本関数はリスク判定という
      決定性重視のタスク特性上 temperature=0 を維持しているが、
      gemini-3.5-flash 移行後に応答品質が不安定な場合は、まず
      temperature を 0.2〜1.0 の範囲で調整して確認すること。
    """
    from langchain_google_vertexai import ChatVertexAI
    gm = _GEMINI_CFG
    kwargs = {
        "model_name":  gm["model"],
        "temperature": 0,
        "location":    gm["location"],
        "max_retries": 1,
    }
    # project が明示的に設定されている場合のみ渡す（ADC 自動検出を優先）
    if gm["project"]:
        kwargs["project"] = gm["project"]
    llm = ChatVertexAI(**kwargs)
    logger.info(f"LLM: Gemini API ({gm['model']}, location={gm['location']}) を使用")
    return llm


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
        temperature          = 0,
        max_retries          = 2,
    )


def build_llm(provider: Optional[str] = None):
    """
    LLM インスタンスを構築して返す。

    LangChain チェーン（RAG / プロンプト）で使用する通常の LLM。
    フォールバック付きの呼び出しは invoke_with_fallback() を使用すること。

    フォールバック順（auto モード）: Groq → Gemini API
    Azure は自動フォールバックには含まれない（LLM_PROVIDER=azure で明示指定時のみ）。

    Args:
        provider: "gemini" | "groq" | "azure" | "auto" | None（None は LLM_PROVIDER 環境変数）
    """
    p = (provider or LLM_PROVIDER).lower()

    if p == "gemini":
        return _build_gemini_llm()

    if p == "azure":
        llm = _build_azure_llm()
        logger.info(f"LLM: Azure OpenAI ({_AZURE_CFG['deployment']}) を使用（明示指定・自動フォールバック対象外）")
        return llm

    if p == "groq":
        # 明示指定モード: フォールバックなし
        llm = _build_groq_llm()
        logger.info(f"LLM: Groq ({GROQ_MODEL}) を使用")
        return llm

    if p == "auto":
        # auto: Groq → Gemini の順で試みる
        if _groq_ok:
            try:
                llm = _build_groq_llm()
                logger.info(f"LLM: Groq ({GROQ_MODEL}) を使用")
                return llm
            except RuntimeError as e:
                logger.warning(f"Groq 初期化失敗 → Gemini にフォールバック: {e}")

        if _gemini_ok:
            try:
                return _build_gemini_llm()
            except Exception as e:
                logger.warning(f"Gemini 初期化失敗: {e}")

        raise RuntimeError(
            "利用可能な LLM プロバイダーがありません。"
            "GROQ_API_KEY または GOOGLE_CLOUD_PROJECT（Gemini ADC）を設定してください。"
        )

    raise ValueError(f"不明な LLM_PROVIDER: {p!r}  (gemini / groq / azure / auto)")


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
    Groq Primary で LLM を呼び出し、失敗時に Gemini Fallback に切り替える。

    LangChain チェーン外で LLM を直接呼び出す場合（task_decompose の
    classify_query 等）に使用する。Azure は自動フォールバックには含まれない。

    Args:
        llm   : build_llm() で構築した LLM インスタンス（Primary）
        prompt: プロンプト文字列

    Returns:
        LLM の応答テキスト
    """
    # Groq / Azure / Gemini 固定モード、または Gemini が使えない場合はフォールバックなし
    if LLM_PROVIDER in ("groq", "azure", "gemini") or not _gemini_ok:
        response = llm.invoke(prompt)
        return response.content if hasattr(response, "content") else str(response)

    # auto モード: Groq (Primary) → Gemini (Fallback) の順
    last_exc = None
    fallbacks = []
    if _gemini_ok:
        fallbacks.append(("Gemini", _build_gemini_llm))

    try:
        t0       = time.time()
        response = llm.invoke(prompt)
        elapsed  = time.time() - t0
        logger.debug(f"LLM 応答: {elapsed:.2f}s")
        return response.content if hasattr(response, "content") else str(response)

    except Exception as e:
        if not _is_fallback_error(e):
            raise   # 構文エラー等はフォールバックしない

        last_exc = e
        logger.warning(
            f"LLM エラー ({type(e).__name__}: {str(e)[:80]}) "
            f"→ フォールバック開始"
        )

    for name, builder in fallbacks:
        try:
            fb_llm   = builder()
            response = fb_llm.invoke(prompt)
            logger.info(f"{name} フォールバック成功")
            return response.content if hasattr(response, "content") else str(response)
        except Exception as e2:
            logger.warning(f"{name} フォールバックも失敗: {e2}")
            last_exc = e2

    raise RuntimeError(
        f"全プロバイダー失敗: {last_exc}"
    ) from last_exc


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

    auto モード: Groq (Primary) → Gemini (Fallback) の with_fallbacks チェーンを構築。
    Azure は自動フォールバックには含まれない。

    使用例:
        llm = build_llm_with_fallback()
        chain = prompt | llm | StrOutputParser()
        result = chain.invoke({"question": "..."})
    """
    primary = build_llm()

    if LLM_PROVIDER not in ("auto",):
        return primary

    # primary が Groq の場合のみ Gemini を LangChain フォールバックとして追加。
    # （Groq キーが無く primary が既に Gemini になっている場合は追加不要）
    fallback_llms = []
    if _groq_ok and _gemini_ok:
        try:
            fallback_llms.append(_build_gemini_llm())
        except Exception as e:
            logger.debug(f"Gemini フォールバック構築スキップ: {e}")

    if not fallback_llms:
        return primary

    return primary.with_fallbacks(
        fallback_llms,
        exceptions_to_handle=(Exception,),
    )


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
    logger.info(f"{prefix}Gemini model  : {_GEMINI_CFG['model']}  "
                f"(project={_GEMINI_CFG['project'] or 'ADC自動検出'}  "
                f"available={'✅' if _gemini_ok else '❌ langchain-google-vertexai 未インストール'})")
    logger.info(f"{prefix}Azure deploy  : {_AZURE_CFG['deployment']}  "
                f"(key={'✅' if _azure_ok else '❌ 未設定'}, 自動フォールバック対象外・明示指定時のみ)")
    if LLM_PROVIDER == "auto" and not _groq_ok and not _gemini_ok:
        logger.warning(f"{prefix}⚠️  利用可能な LLM プロバイダーがありません（Groq/Geminiともに未設定）")
    elif LLM_PROVIDER == "auto" and _groq_ok != _gemini_ok:
        logger.warning(f"{prefix}⚠️  Groq/Gemini のどちらか一方のみ利用可能 — フォールバックが機能しません")
