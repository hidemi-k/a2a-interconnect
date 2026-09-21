#!/usr/bin/env python3
# Copyright (c) 2026 hidemi-k
# Licensed under the MIT License.
"""
interconnect_bridge_a2a_server.py — Interconnect Bridge Agent
================================================================
Connection Coordinator API（OpenAPI 3.0 Interconnect）のProvider役を
A2A化するBridge Agent。同一コードをPROVIDER_ID違いで2プロセス起動する。

【状態遷移】
  PENDING → GUIDANCE_ISSUED → FEATURE_PROPOSED → FEATURE_ACCEPTED
    → (governance PERMIT) → DEPLOYING → VERIFIED
    → (governance REVIEW) → AWAITING_HUMAN_APPROVAL → [人間が/approve] → DEPLOYING → VERIFIED
    → (governance DENY)   → BLOCKED

【環境変数】
  A2A_PORT          : このプロセスのポート（8202、Integration Layer帯域）
  PROVIDER_ID        : "aws" | "gcp" 等、自分の役割名
  PEER_BASE_URL_{NAME} : peer_provider="{name}"（小文字化）に対応する接続先URL。
                       設定した分だけ自動的にPEER_BASE_URL_MAPへ登録される。
                       "自分がどちらの役か"ではなく"相手が誰か"がキーになる点に注意
                       （aws役プロセスには PEER_BASE_URL_GCP を、
                        gcp役プロセスには PEER_BASE_URL_AWS を設定する）。
  PEER_BASE_URL      : 非推奨（廃止予定）。PEER_BASE_URL_{NAME}を使うこと。
  CEOS_HUB_URL       : a2a-ceos-core Hub（デフォルト: http://localhost:8000）
  JUNOS_HUB_URL      : a2a-junos-core Hub（デフォルト: http://localhost:8020）
  VENDOR_ID          : 現在このProvider役が使うベンダー（デフォルト: "ceos"）
  JUNOS_IFACE        : Junos向け対象インターフェース名（デフォルト: "et-0/0/2"）
  DEVICE_IP          : 自分が担当する cEOS の管理IP（ceos1 or ceos2）
  DEVICE_USERNAME / DEVICE_PASSWORD : cEOSログイン情報
  GOVERNANCE_A2A_URL : governance_client.py が参照（デフォルト: http://localhost:8190）
  HTTP_TIMEOUT       : 下流呼び出しのタイムアウト秒（デフォルト: 30）
  A2A_PUBLIC_URL     : Agent Cardに載せる公開URL（デフォルト: http://localhost:{A2A_PORT}）

"""

import asyncio
import itertools
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import json

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import (
    add_a2a_routes_to_fastapi,
    create_agent_card_routes,
    create_jsonrpc_routes,
    create_rest_routes,
)
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill
from a2a.utils import TransportProtocol
from a2a.helpers import new_text_message

from governance_client import get_governance_client
from response_schema import (
    STATUS_SUCCESS, STATUS_ERROR,
    make_response, make_error_response, is_ok,
)
from llm_factory import build_llm_with_fallback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("interconnect_bridge")

# ── 設定 ──────────────────────────────────────────────────────────────────────
VERSION = "0.2.0"  # v0.2.0: /deploy_structured対応（概要設計書5.6節、v0.11反映）

A2A_HOST     = os.getenv("A2A_HOST", "0.0.0.0")
A2A_PORT     = int(os.getenv("A2A_PORT", "8202"))  # 恒久稼働ポート（Integration Layer）
A2A_PUBLIC_URL = os.getenv("A2A_PUBLIC_URL", f"http://localhost:{A2A_PORT}")
PROVIDER_ID  = os.getenv("PROVIDER_ID", "aws")
# ★ PEER_BASE_URL_MAP: peer_provider ごとの接続先URL（VENDOR_HUB_MAPと同じ発想）。
#   複数プロバイダを1プロセスで一元処理できるようにするための解決。
#   以前は PEER_BASE_URL という単一のグローバル変数で1つの相手先しか
#   扱えなかったが、peer_provider は本来リクエストごとに変わる値であり、
#   接続先URLもそれに応じて変わるべきだった（実運用のConnection Coordinator
#   APIは1プロセスで複数の相手と交渉できる設計が前提）。
#
#   ローカル検証時、本物の外部アクセス手段がない場合のみ、相手役ダブルとして
#   同一コードをもう1プロセス起動する。そのダブルは恒久登録されたIntegration
#   Layerのポートではなく、a2aポート台帳.md 1.1節の慣習に従い8290番台を使う
#   （splunk_bridge_ui.py 等のデモ専用UIと同じ扱い）。
#
# ★ PEER_BASE_URL_MAP: peer_provider ごとの接続先URL（VENDOR_HUB_MAPと同じ発想）。
#   複数プロバイダを1プロセスで一元処理できるようにするための解決。
#   以前は PEER_BASE_URL という単一のグローバル変数で1つの相手先しか
#   扱えなかったが、peer_provider は本来リクエストごとに変わる値であり、
#   接続先URLもそれに応じて変わるべきだった（実運用のConnection Coordinator
#   APIは1プロセスで複数の相手と交渉できる設計が前提）。
#
#   ローカル検証時、本物の外部アクセス手段がない場合のみ、相手役ダブルとして
#   同一コードをもう1プロセス起動する。そのダブルは恒久登録されたIntegration
#   Layerのポートではなく、a2aポート台帳.md 1.1節の慣習に従い8290番台を使う
#   （splunk_bridge_ui.py 等のデモ専用UIと同じ扱い）。
#
#   ★バグ修正（①最終確認テストで発見）: 当初は辞書のキーを "gcp" に決め打ち
#   していたため、gcp役プロセス（PROVIDER_ID=gcp）から見た相手（"aws"）を
#   一切解決できず、NotifyConnectionStatus送信時に必ず失敗していた。
#   「自分が今どちらの役か」によって、必要な相手先キーは変わる。
#   そのため、環境変数 PEER_BASE_URL_{PEER_PROVIDER大文字} が設定されている
#   ものだけを動的に登録する方式に変更した。
#   新しい peer_provider を追加する場合は PEER_BASE_URL_{NAME} という
#   環境変数を1つ足すだけでよい（例: PEER_BASE_URL_AZURE=http://localhost:8291）。
PEER_BASE_URL = os.getenv("PEER_BASE_URL", "")  # 後方互換用（新規は使わないこと）
_PEER_ENV_PREFIX = "PEER_BASE_URL_"
PEER_BASE_URL_MAP: Dict[str, str] = {
    key[len(_PEER_ENV_PREFIX):].lower(): value
    for key, value in os.environ.items()
    if key.startswith(_PEER_ENV_PREFIX) and value
}
if PEER_BASE_URL and not PEER_BASE_URL_MAP:
    # 後方互換: PEER_BASE_URL_* が1つも設定されていない環境向けの最終フォールバック。
    # peer_provider名が分からないため "gcp" 固定は行わず、警告だけ出す。
    logger.warning(
        "PEER_BASE_URL は非推奨です。PEER_BASE_URL_{PEER_PROVIDER} 形式の"
        "環境変数（例: PEER_BASE_URL_GCP）を使用してください。"
    )

# ★仕様準拠フェーズA: 本物のInterconnect仕様（feature.yaml の
#   providerBgpConfigs）は「プロバイダごとに固有の実在ASN」を前提として
#   おり、動的採番すべき値ではない。PEER_BASE_URL_MAPと同じ環境変数駆動の
#   パターンで、各providerの実ASNを設定できるようにする。
#   例: PROVIDER_ASN_AWS=65001 PROVIDER_ASN_GCP=65002
#   （両方の値を、両プロセスに設定する必要がある。現状はResponder側が
#   両者のASNをまとめて決めてしまう設計のため。フェーズBで是正予定）。
_ASN_ENV_PREFIX = "PROVIDER_ASN_"
PROVIDER_ASN_MAP: Dict[str, int] = {
    key[len(_ASN_ENV_PREFIX):].lower(): int(value)
    for key, value in os.environ.items()
    if key.startswith(_ASN_ENV_PREFIX) and value
}


def _resolve_peer_base_url(peer_provider: str) -> str:
    """peer_provider に対応する接続先URLを解決する。未登録なら明示的に例外にする。"""
    url = PEER_BASE_URL_MAP.get(peer_provider)
    if url is None:
        raise ValueError(
            f"未知の peer_provider '{peer_provider}'（PEER_BASE_URL_MAP に未登録）。"
            "新しい交渉相手を追加する場合は PEER_BASE_URL_MAP にエントリを追加すること。"
        )
    return url


DEVICE_IP       = os.getenv("DEVICE_IP", "172.20.100.31")
DEVICE_USERNAME = os.getenv("DEVICE_USERNAME", "admin")
DEVICE_PASSWORD = os.getenv("DEVICE_PASSWORD", "admin")

# ── ベンダー切替マップ ────────────────────────────────────────────────────────
# 「対応ベンダーが増える」場合はここだけを変更する（新規ポートは不要）。
# 概要設計書 4.5節「将来拡張時の指針」参照。
# このProvider役が現在どのベンダーコアへデプロイするかを VENDOR_ID で選択する。
VENDOR_ID = os.getenv("VENDOR_ID", "junos")  # "ceos" | "junos" | 今後追加するベンダー
                                              # ★現フェーズはJunos⇔Junos限定（5.8節）のため
                                              #   デフォルトをjunosに変更（レビュー反映）。
                                              #   VENDOR_ID未設定のままceos-coreが選ばれる
                                              #   事故を防ぐ。
VENDOR_HUB_MAP: Dict[str, str] = {
    "ceos":  os.getenv("CEOS_HUB_URL",  "http://localhost:8000"),
    "junos": os.getenv("JUNOS_HUB_URL", "http://localhost:8020"),
    # 新規ベンダー追加時はここに1行足すだけでよい（例: "iosxe": "http://localhost:8040"）
}
# Junos向け対象インターフェース名（実トポロジー: cjunosevo1 の et-0/0/2 が ceos1:eth1 と対向）
JUNOS_IFACE = os.getenv("JUNOS_IFACE", "et-0/0/2")

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))

# ★実機検証で判明: LLMベースのXML生成（特にJunosのVLAN関連設定）は
#   同一クエリでも生成結果が揺れる非決定性を持つ。単純な自動リトライで
#   救えるケースが多いことを実機で確認したため、失敗コマンドのみ
#   再試行する仕組みを追加する。
#
# ★概要設計書 v0.11 での位置づけ変更: 上記はNL経路（_build_write_commands_*
#   → _deploy_via_vendor_hub、VENDOR_STRUCTURED_TASK_BUILDERS未登録のベンダー
#   向けフォールバック）にのみ当てはまる。/deploy_structured を使う決定論的
#   経路（_build_structured_tasks_junos → _deploy_via_vendor_hub_structured）
#   では、LLMによるXML生成自体が発生しないため、このリトライはNETCONFの
#   ロック競合や一時的な接続断といったインフラ起因の失敗のみを対象とする
#   （詳細は _deploy_via_vendor_hub_structured() のdocstring参照）。
DEPLOY_MAX_ATTEMPTS = int(os.getenv("DEPLOY_MAX_ATTEMPTS", "3"))
DEPLOY_RETRY_DELAY_SEC = float(os.getenv("DEPLOY_RETRY_DELAY_SEC", "2"))

app = FastAPI(title=f"a2a-interconnect ({PROVIDER_ID})", version=VERSION)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# 接続状態のインメモリストア（trace_id/connection_id をキーにする）
_connections: Dict[str, Dict[str, Any]] = {}


# ═══════════════════════════════════════════════════════════════════════════════
# 決定論的な L3 パラメータ採番（LLM不使用。schemas/connection.yaml の
# L3BaseGuidance レンジをベースに未使用の先頭を割り当てる）
# ═══════════════════════════════════════════════════════════════════════════════

class L3Allocator:
    """
    ASN / VLAN / Subnet の範囲提示（FeatureGuidance）をデモ用に固定レンジから
    決定論的に行う。
    """
    _VLAN_BASE = 2000  # ★仕様準拠: 1024以下は予約済みのため、それより十分大きい値に変更
    _VLAN_RANGE_SIZE = 10       # 1回のFeatureGuidanceで提示するVLAN範囲の幅
    _SUBNET_THIRD_OCTET_BASE = 100  # 169.254.100.0/30, 169.254.101.0/30, ...
    _MTU_MIN = 1500
    _MTU_MAX = 9000

    def __init__(self):
        self._counter = itertools.count()

    def generate_guidance(self, provider: str, peer_provider: str) -> Dict[str, Any]:
        """
        FeatureGuidance（仕様準拠、feature.yaml の L3BaseGuidance相当）を
        生成する。ここではResponder自身のASN・ホストIPの範囲・VLAN範囲・
        MTU範囲のみを提示し、Negotiator側が範囲内から実際の値を選択する。

        Args:
            provider:      自分（Responder）のidentity（"gcp"等）
            peer_provider: 相手（Negotiator）のidentity（"aws"等）

        Returns:
            {"vlanRange": {...}, "asnRange": {...}, "ipv4SubnetGuidance": {...},
             "mtuRange": {...}, "own_asn": int, "peer_asn": int}
            own_asn/peer_asnはガイダンス自体には含めず、Negotiatorが最終的な
            providerBgpConfigsを組み立てる際に使う参考情報として別途返す
            （実在ASNは範囲交渉の対象ではないため）。
        """
        n = next(self._counter)
        third_octet = self._SUBNET_THIRD_OCTET_BASE + n
        vlan_start = self._VLAN_BASE + n * self._VLAN_RANGE_SIZE

        own_asn = PROVIDER_ASN_MAP.get(provider)
        peer_asn = PROVIDER_ASN_MAP.get(peer_provider)
        if own_asn is None:
            raise ValueError(f"PROVIDER_ASN_MAP に自分（'{provider}'）のASNが未登録です")
        if peer_asn is None:
            raise ValueError(f"PROVIDER_ASN_MAP に相手（'{peer_provider}'）のASNが未登録です")

        return {
            "vlanRange":  {"start": vlan_start, "end": vlan_start + self._VLAN_RANGE_SIZE - 1},
            "mtuRange":   {"start": self._MTU_MIN, "end": self._MTU_MAX},
            "ipv4SubnetGuidance": {
                "includeSubnet": f"169.254.{third_octet}.0/30",
            },
            "own_asn":  own_asn,   # Responder自身の実在ASN（範囲ではなく確定値）
            "peer_asn": peer_asn,  # Negotiator側の実在ASN（Responderが事前に把握している前提）
        }


def _select_from_guidance(guidance: Dict[str, Any], provider: str, peer_provider: str) -> Dict[str, Any]:
    """
    仕様準拠フェーズB: Negotiator側が、Responderの提示した「範囲」から
    実際の値を決定論的に選択する。選択戦略は「範囲の最小値を選ぶ」に統一
    する（設計討議で確認済み）。

    Args:
        guidance: L3Allocator.generate_guidance() が返した辞書
        provider: 自分（Negotiator）のidentity
        peer_provider: 相手（Responder）のidentity

    Returns:
        仕様のL3BaseConfig（feature.yaml）に準拠した確定済みFeatureConfig。
    """
    vlan_id = guidance["vlanRange"]["start"]
    mtu = guidance["mtuRange"]["start"]
    include_subnet = guidance["ipv4SubnetGuidance"]["includeSubnet"]
    network, prefix_len = include_subnet.split("/")
    third_octet = network.split(".")[2]

    return {
        "vlanId":                 vlan_id,
        "ipv4Subnet":             network,
        "ipv4SubnetPrefixLength": int(prefix_len),
        "mtuBytes":               mtu,
        "md5BgpPassword":         uuid.uuid4().hex[:16],
        "providerBgpConfigs": [
            # ★ guidanceの own_asn は Responder（=peer_provider視点でのown）を
            #   指すため、Negotiator視点では「相手のASN」になる。
            {"provider": provider,      "asn": guidance["peer_asn"], "hostIpv4": f"169.254.{third_octet}.1"},
            {"provider": peer_provider, "asn": guidance["own_asn"],  "hostIpv4": f"169.254.{third_octet}.2"},
        ],
    }


_allocator = L3Allocator()




# ═══════════════════════════════════════════════════════════════════════════════
# リクエストモデル（Connection Coordinator API のサブセット）
# ═══════════════════════════════════════════════════════════════════════════════

class CreateConnectionRequest(BaseModel):
    environment: str
    bandwidth_mbps: int
    peer_provider: str


class AcceptConnectionRequest(BaseModel):
    """
    仕様準拠フェーズB（5.13節）: 顧客（エージェント）がResponder側で
    接続を受け入れる操作。Fabric One流の自然言語入力を想定するが、
    activation_key等の重要な値はLLM抽出させず構造化パラメータで渡す
    （create_connection_nlと同じ安全原則）。
    """
    text: str = ""  # 自然言語での意思表示（監査ログ用、値の抽出には使わない）
    activation_key: str
    connection_id: str
    negotiator_provider: str  # ActivationKeyを発行した側（Negotiator）のidentity


class VerifyActivationKeyRequest(BaseModel):
    """仕様準拠フェーズB: Responder→NegotiatorへのConfirmActivationKey相当（B→A方向）。"""
    connection_id: str
    activation_key: str
    requesting_provider: str  # 検証を依頼してきた側（Responder）のidentity


class GenerateFeatureGuidanceRequest(BaseModel):
    """仕様準拠フェーズB: Negotiator→ResponderへのGenerateFeatureGuidance要求。"""
    connection_id: str
    requesting_provider: str  # 要求元（Negotiator）のidentity


class CreateFeatureRequest(BaseModel):
    connection_id: str
    proposed_config: Dict[str, Any]
    propagate: bool = True


class NotifyConnectionStatusRequest(BaseModel):
    connection_id: str
    state: str  # "VERIFIED" | "FAILED"


class ApproveRequest(BaseModel):
    connection_id: str
    operator_note: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════════
# 下流サービス呼び出しヘルパー
# ═══════════════════════════════════════════════════════════════════════════════

async def _post(url: str, payload: dict) -> dict:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()


def _find_provider_bgp_config(feature_config: Dict[str, Any], provider: str) -> Dict[str, Any]:
    """
    仕様準拠フェーズAで導入。providerBgpConfigs配列（feature.yaml準拠）から
    指定providerのエントリを取り出す共通ヘルパー。
    """
    for entry in feature_config.get("providerBgpConfigs", []):
        if entry.get("provider") == provider:
            return entry
    raise KeyError(f"providerBgpConfigsに provider='{provider}' のエントリが見つかりません")


def _find_peer_bgp_config(feature_config: Dict[str, Any], own_provider: str) -> Dict[str, Any]:
    """providerBgpConfigsの中から、自分以外（＝相手）のエントリを取り出す。"""
    for entry in feature_config.get("providerBgpConfigs", []):
        if entry.get("provider") != own_provider:
            return entry
    raise KeyError(f"providerBgpConfigsに '{own_provider}' 以外のエントリが見つかりません")


def _build_write_commands_ceos(feature_config: Dict[str, Any]) -> list[str]:
    """
    Arista cEOS向け自然言語コマンド生成。

    🔑 コマンド生成の設計原則: _build_write_commands_junos() のdocstring
    参照。「異なるタスク種別の混在は避ける」原則に基づく。

    ⚠️ 実機検証で判明（次回要検証）: 「Ethernet1.{vlan}のIPv4アドレスを
    設定し、MTUを設定して」の1文が
    "subinterfaces with index > 0 cannot be configured as bridged ports"
    で失敗した。Junosのvlan-tagging問題と構造的に同じ「L3として明示宣言
    する手順が抜けている」問題の可能性が高い。次回、Junosと同様に
    「L3宣言（dot1qカプセル化等）→IPv4アドレス→MTU」の3ステップに分割
    する案を検証すること（現時点ではまだ2ステップのまま、未検証）。
    """
    vlan    = feature_config["vlanId"]
    mtu     = feature_config["mtuBytes"]
    own_bgp  = _find_provider_bgp_config(feature_config, PROVIDER_ID)
    peer_bgp = _find_peer_bgp_config(feature_config, PROVIDER_ID)
    host_ip = own_bgp["hostIpv4"]
    peer_ip = peer_bgp["hostIpv4"]
    asn     = peer_bgp["asn"]  # ルータBGPネイバーには相手のASNを指定する

    return [
        f"Ethernet1にVLAN {vlan}を作成して、Interconnect-{PROVIDER_ID}という名前を設定して",
        f"Ethernet1.{vlan}のIPv4アドレスを{host_ip}/{feature_config['ipv4SubnetPrefixLength']}に設定し、MTUを{mtu}にして",
        f"ルータBGP {asn}にネイバー{peer_ip}を追加して",
    ]


def _build_write_commands_junos(feature_config: Dict[str, Any]) -> list[str]:
    """
    Junos向け自然言語コマンド生成。

    🔑 コマンド生成の設計原則（実機検証で確定・全ベンダー共通の指針）:
    ネットワークOS側のa2a Hub（task_decompose相当のタスク分解ロジック）は、
    「同一タスク種別の複数インスタンス」（例: BGPネイバーのgroup+neighbor+AS
    のような、1つの操作に対する複数属性の設定）は1文にまとめても正しく
    処理できるが、「異なるタスク種別の混在」（例: インターフェースの
    L2/L3属性設定 と VLAN所属設定 のように、性質の異なる2つの設定変更を
    1文に混ぜる）は、矛盾した複数の<interface>ブロックを生成する等、
    ほぼ確実に失敗することが実機で確認された（Junos: vlan-tagging+vlan-id
    の混在、cEOS: IPv4アドレス+MTUの混在で同様の失敗パターン）。

    そのため、本関数（および他ベンダー向けの _build_write_commands_*）は、
    タスク種別が変わる境界では必ずコマンドを分割する。同一タスク種別内の
    複数属性はまとめてよい（BGPの例を参照）。新規ベンダー追加時もこの
    原則を踏襲すること。

    ✅ 実機検証で確定（v3、cjunosevo1 / EVO 25.4R1.13で全項目デプロイ成功）:
      - 「VLAN」という単語を含めると、RAG検索が誤って switchport 系の
        family ethernet-switching テンプレートを引き当て、vlan-tagging /
        vlan-id のどちらも syntax error になることが複数回の実機試行で
        判明した（同一クエリでも結果が揺らぐ非決定性も確認）。
      - 「VLAN」を避け「802.1Qタグ付け」「vlan-id」という表現に置き換える
        ことで、family非依存のシンプルな正しいXMLが安定して生成された。
      - ただし後日、VLAN番号の値自体（例: 100）によって、同じ言い回しでも
        誤ったXMLが安定して再現するケースを確認した。言い回しだけでなく
        値との組み合わせでRAG検索結果が変わる可能性があり、根本的な安定
        化には junos_hub_a2a_server.py 側のRAGテンプレート改善が必要。
      - vlan-tagging は物理インターフェース直下の単純フラグであり、
        有効化前は当該インターフェースの全unitがタグ付きである必要が
        あるため、既存の untagged unit（例: unit 0）があると
        コミット拒否される（Junos自体の仕様、テンプレートの問題ではない）。
        対象インターフェースは事前にタグなしunitが存在しない状態に
        しておくこと。

    JUNOS_IFACE（環境変数）で対象インターフェース名を指定する
    （デフォルト: et-0/0/2、今回の検証トポロジーに合わせた値）。
    """
    vlan    = feature_config["vlanId"]
    mtu     = feature_config["mtuBytes"]
    own_bgp  = _find_provider_bgp_config(feature_config, PROVIDER_ID)
    peer_bgp = _find_peer_bgp_config(feature_config, PROVIDER_ID)
    host_ip = own_bgp["hostIpv4"]
    peer_ip = peer_bgp["hostIpv4"]
    asn     = peer_bgp["asn"]  # BGPネイバーには相手のASNを指定する
    iface   = JUNOS_IFACE

    return [
        f"{iface} のMTUを{mtu}に設定して",
        f"{iface} の802.1Qタグ付けを有効にして",
        f"{iface}のunit {vlan}のvlan-idを{vlan}に設定して",
        f"{iface}のunit {vlan}にIPv4アドレス{host_ip}/{feature_config['ipv4SubnetPrefixLength']}を設定して",
        f"BGPグループ INTERCONNECT-{PROVIDER_ID} にneighbor {peer_ip}を追加して、AS {asn}を設定して",
    ]


VENDOR_COMMAND_BUILDERS = {
    "ceos":  _build_write_commands_ceos,
    "junos": _build_write_commands_junos,
    # 新規ベンダー追加時はここに1関数追加するだけでよい
}


# ═══════════════════════════════════════════════════════════════════════════════
# 決定論的経路（概要設計書5.6節）: 構造化タスクビルダー
# ─────────────────────────────────────────────────────────────────────────────
# 自然言語コマンド（_build_write_commands_*）の代わりに、a2a-junos-core側の
# 新設エンドポイント /deploy_structured にそのまま渡せる構造化パラメータを
# 生成する。LLM+RAGを一切経由しないため、_build_write_commands_junos()の
# docstringで報告されている「VLANという単語によるRAG誤誘発」「VLAN番号の
# 値依存の非決定性」は原理的に発生しない。
#
# タスク種別の分割は _build_write_commands_junos() の5分割（MTU→802.1Q
# タグ付け→vlan-id→IPv4→BGP）をそのまま踏襲する（v0.11で確定した「タスク
# 種別が変わる境界では必ず分割する」というルールに基づく。a2a-junos-core
# 側の _classify_governance_action() がXML内容をキーワード判定するため、
# 複数タスク種別を1回のfinal_xmlにまとめるとgovernance分類・auditが
# 正しく機能しない）。
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# ②試験実装：3層分離（Capability分類 → DAG生成 → 実行）— Junos限定
# ─────────────────────────────────────────────────────────────────────────────
# 「DAGの応用」資料の指摘を反映: DAGは依存関係（順序）は表現できるが、
# 「このタスクは何者か」という分類はDAGの管轄外であり、両者を1つの
# ハードコードされた固定リストに混ぜると、タスク種別が増えたときに
# 破綻する（5.4節で発見した非決定性の根本原因も同種の混同だった）。
#
# 第1層: Capability Ontology — 「このFeatureConfigはどのCapabilityを
#         必要とするか」を判定する（`applies_if`）。将来的にInterconnect
#         スコープが広がった際（MACsec・メンテナンス調整等）も、この
#         レジストリにエントリを追加するだけで済む設計を意図している。
# 第2層: DAG生成 — `depends_on`で宣言された依存関係をトポロジカルソートし、
#         実行順序を導出する。順序はコードに直接書かず、依存関係の宣言から
#         機械的に導かれる。
# 第3層: 実行 — 既存の VENDOR_STRUCTURED_TASK_BUILDERS /
#         _deploy_via_vendor_hub_structured() をそのまま流用する（無改造）。
#
# ⚠️ 試験実装のスコープ（Junos限定）: この3層分離パターン自体は
#   a2a-junos-coreのHub（_classify_governance_action等）にも将来応用
#   できる可能性があるが、今回はa2a-interconnect前段のみで試験する
#   （5.10節の議論を参照。Hub側は汎用オントロジー、こちらはConnection
#   Coordinator API仕様に閉じた狭いサブセットという住み分け）。
# ═══════════════════════════════════════════════════════════════════════════════

CAPABILITY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "interconnect.l2.mtu": {
        "task_type":   "interface_mtu",
        "depends_on":  [],
        "applies_if":  lambda cfg: "mtuBytes" in cfg,
        "params":      lambda cfg: {"interface": JUNOS_IFACE, "mtu": cfg["mtuBytes"]},
        "description": lambda cfg: f"{JUNOS_IFACE} のMTUを{cfg['mtuBytes']}に設定",
    },
    "interconnect.l2.dot1q_enable": {
        "task_type":   "interface_dot1q_enable",
        "depends_on":  [],
        "applies_if":  lambda cfg: "vlanId" in cfg,
        "params":      lambda cfg: {"interface": JUNOS_IFACE},
        "description": lambda cfg: f"{JUNOS_IFACE} の802.1Qタグ付けを有効化",
    },
    "interconnect.l2.vlan_id": {
        "task_type":   "interface_vlan_id",
        "depends_on":  ["interconnect.l2.dot1q_enable"],
        "applies_if":  lambda cfg: "vlanId" in cfg,
        "params":      lambda cfg: {"interface": JUNOS_IFACE, "unit": cfg["vlanId"], "vlan_id": cfg["vlanId"]},
        "description": lambda cfg: f"{JUNOS_IFACE}.{cfg['vlanId']} のvlan-idを{cfg['vlanId']}に設定",
    },
    "interconnect.l3.ipv4": {
        "task_type":   "interface_ipv4",
        "depends_on":  ["interconnect.l2.vlan_id"],
        "applies_if":  lambda cfg: "providerBgpConfigs" in cfg and "vlanId" in cfg,
        "params":      lambda cfg: {
            "interface": JUNOS_IFACE,
            "unit": cfg["vlanId"],
            "ipv4_address": f"{_find_provider_bgp_config(cfg, PROVIDER_ID)['hostIpv4']}/{cfg['ipv4SubnetPrefixLength']}",
        },
        "description": lambda cfg: f"{JUNOS_IFACE}.{cfg['vlanId']} にIPv4アドレス"
                                    f"{_find_provider_bgp_config(cfg, PROVIDER_ID)['hostIpv4']}/{cfg['ipv4SubnetPrefixLength']}を設定",
    },
    "interconnect.l3.bgp_neighbor": {
        "task_type":   "bgp_neighbor",
        # ★意味的な依存関係: BGPネイバーはIPv4アドレスが設定されて初めて
        #   実際に確立しうる。既存の固定リスト版でも最後に置かれていたが、
        #   そこでは「たまたま最後に書いた」だけで、依存関係として宣言は
        #   されていなかった。ここで明示化する。
        "depends_on":  ["interconnect.l3.ipv4"],
        "applies_if":  lambda cfg: "providerBgpConfigs" in cfg,
        "params":      lambda cfg: {
            "group": f"INTERCONNECT-{PROVIDER_ID}",
            "neighbor_ipv4": _find_peer_bgp_config(cfg, PROVIDER_ID)["hostIpv4"],
            "peer_as": _find_peer_bgp_config(cfg, PROVIDER_ID)["asn"],
        },
        "description": lambda cfg: f"BGPグループ INTERCONNECT-{PROVIDER_ID} に neighbor "
                                    f"{_find_peer_bgp_config(cfg, PROVIDER_ID)['hostIpv4']}"
                                    f"（AS {_find_peer_bgp_config(cfg, PROVIDER_ID)['asn']}）を追加",
    },
    # 新しいcapabilityを追加する場合はここに1エントリ足すだけでよい
    # （例: 将来のMACsec鍵ローテーション等、5.8節スコープ外事項の追加時）。
}

# ── cEOS向けCapability定義（5.15節、実機検証済みの3タスクのみ登録） ──────────
# ⚠️ interface_mtu は実機検証で発見した課題（openconfig-interfaces:mtuが
#   `l2 mtu`コマンドに変換されるが、L3ルーテッドモードでは非サポート）が
#   未解決のため、当面はここに登録しない（5.5節・5.15節参照）。
CEOS_IFACE = os.getenv("CEOS_IFACE", "Ethernet1")

CEOS_CAPABILITY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "interconnect.l2.vlan": {
        "task_type":   "create_vlan",
        "depends_on":  [],
        "applies_if":  lambda cfg: "vlanId" in cfg,
        "params":      lambda cfg: {"vlan_id": cfg["vlanId"], "name": f"Interconnect-{PROVIDER_ID}"},
        "description": lambda cfg: f"VLAN {cfg['vlanId']} を作成し、Interconnect-{PROVIDER_ID}という名前を設定",
    },
    "interconnect.l3.ipv4": {
        "task_type":   "interface_ipv4",
        "depends_on":  ["interconnect.l2.vlan"],
        "applies_if":  lambda cfg: "providerBgpConfigs" in cfg and "vlanId" in cfg,
        "params":      lambda cfg: {
            "interface": CEOS_IFACE,
            "unit": cfg["vlanId"],
            "ip": _find_provider_bgp_config(cfg, PROVIDER_ID)["hostIpv4"],
            "prefix_length": cfg["ipv4SubnetPrefixLength"],
        },
        "description": lambda cfg: f"{CEOS_IFACE}.{cfg['vlanId']} にIPv4アドレス"
                                    f"{_find_provider_bgp_config(cfg, PROVIDER_ID)['hostIpv4']}を設定",
    },
    "interconnect.l3.bgp_neighbor": {
        "task_type":   "bgp_neighbor",
        "depends_on":  ["interconnect.l3.ipv4"],
        "applies_if":  lambda cfg: "providerBgpConfigs" in cfg,
        "params":      lambda cfg: {
            "neighbor_ipv4": _find_peer_bgp_config(cfg, PROVIDER_ID)["hostIpv4"],
            "peer_as": _find_peer_bgp_config(cfg, PROVIDER_ID)["asn"],
        },
        "description": lambda cfg: f"BGPネイバー {_find_peer_bgp_config(cfg, PROVIDER_ID)['hostIpv4']}"
                                    f"（AS {_find_peer_bgp_config(cfg, PROVIDER_ID)['asn']}）を追加",
    },
    # interface_mtu は未解決のため未登録（5.5節参照）。解決次第ここに追加する。
}


def _resolve_capability_dag(feature_config: Dict[str, Any], registry: Dict[str, Dict[str, Any]]) -> list[Dict[str, Any]]:
    """
    第1層（Capability判定）→第2層（DAG生成・トポロジカルソート）を実施し、
    第3層（実行）に渡せるタスク列を返す。

    ★汎用化（v0.27、ceos対応）: 従来はグローバル変数CAPABILITY_REGISTRY
    （Junos専用）を直接参照していたが、cEOS向けにも同じ仕組みを使うため
    registry引数で切り替えられるようにした。

    Args:
        feature_config: 仕様のFeatureConfig（providerBgpConfigs配列等）
        registry: CAPABILITY_REGISTRY または CEOS_CAPABILITY_REGISTRY

    Returns:
        [{"task_type": str, "params": dict, "description": str}, ...]
        （既存の _build_structured_tasks_junos() と同じ出力形式）
    """
    applicable = [name for name, spec in registry.items() if spec["applies_if"](feature_config)]
    applicable_set = set(applicable)

    resolved: list[str] = []
    visiting: set = set()

    def visit(name: str) -> None:
        if name in resolved:
            return
        if name in visiting:
            raise ValueError(f"Capability依存関係に循環を検出: {name}")
        visiting.add(name)
        for dep in registry[name]["depends_on"]:
            if dep in applicable_set:
                visit(dep)
            elif dep not in registry:
                raise ValueError(f"未登録のcapabilityへの依存: {name} → {dep}")
        visiting.discard(name)
        resolved.append(name)

    for name in applicable:
        visit(name)

    return [
        {
            "task_type":   registry[name]["task_type"],
            "params":      registry[name]["params"](feature_config),
            "description": registry[name]["description"](feature_config),
        }
        for name in resolved
    ]


def _build_structured_tasks_junos(feature_config: Dict[str, Any]) -> list[Dict[str, Any]]:
    """
    Junos向け構造化タスク列を生成する（決定論的、LLM不使用）。

    ✅ v0.22で3層分離（Capability分類→DAG生成→実行）に置き換え済み。
    実行順序はもはやこの関数内にハードコードされておらず、
    CAPABILITY_REGISTRY の depends_on 宣言から機械的に導出される。
    実機検証済みの既存順序（MTU→802.1Qタグ付け→vlan-id→IPv4→BGP）と
    導出結果が一致することを確認済み（11.-2節参照）。
    """
    return _resolve_capability_dag(feature_config, CAPABILITY_REGISTRY)


def _build_structured_tasks_ceos(feature_config: Dict[str, Any]) -> list[Dict[str, Any]]:
    """
    cEOS向け構造化タスク列を生成する（決定論的、LLM不使用）。

    ✅ v0.27で新設。JunosのCAPABILITY_REGISTRY/DAG方式をそのまま流用し、
    CEOS_CAPABILITY_REGISTRYに差し替えただけ（3層分離パターンがJunos
    限定でなく再利用可能な設計だったことの実証）。
    interface_mtuは既知の未解決事項のため対象外（5.5節・5.15節参照）。
    """
    return _resolve_capability_dag(feature_config, CEOS_CAPABILITY_REGISTRY)


# 決定論的経路（/deploy_structured）が実装済みのベンダーのみここに登録する。
# 未登録のベンダーは、_execute_deployment() が自動的に旧来のNL経路
# （VENDOR_COMMAND_BUILDERS + _deploy_via_vendor_hub）にフォールバックする。
VENDOR_STRUCTURED_TASK_BUILDERS: Dict[str, Any] = {
    # ✅ 【有効化済み・①完結で実機確認済み】a2a-junos-core側の/deploy_structured
    #   実装・単体疎通確認・a2a-interconnectからのエンドツーエンド検証
    #   （create_connection→create_feature→approve→VERIFIED到達）を
    #   完了したため有効化した（設計書11.-1節「①完結宣言」参照）。
    "junos": _build_structured_tasks_junos,
    # ✅ 【有効化済み・v0.27】a2a-ceos-core側の/deploy_structured実装・
    #   単体疎通確認（create_vlan/interface_ipv4/bgp_neighborの3タスク）を
    #   完了したため有効化した。interface_mtuは未解決のため対象外
    #   （CEOS_CAPABILITY_REGISTRYに未登録）。
    "ceos": _build_structured_tasks_ceos,
}


def _build_write_commands(feature_config: Dict[str, Any]) -> list[str]:
    """
    確定した FeatureConfig を、現在の VENDOR_ID に応じたベンダー別の
    自然言語コマンド列に変換する（決定論的テンプレート、LLM不使用）。

    概要設計書 4.5節「将来拡張時の指針」で示した通り、対応ベンダーが
    増えてもポートは増やさず、この関数のディスパッチ先を増やすだけで
    対応する。
    """
    builder = VENDOR_COMMAND_BUILDERS.get(VENDOR_ID)
    if builder is None:
        raise RuntimeError(f"未知の VENDOR_ID '{VENDOR_ID}'（VENDOR_COMMAND_BUILDERS に未登録）")
    return builder(feature_config)


async def _deploy_via_vendor_hub(query: str) -> dict:
    """
    現在の VENDOR_ID に対応するベンダーコアの A2A Hub に自然言語コマンドを
    投入し、Dry-run(/execute) → 実機投入(/deploy/{trace_id}) の2段階で実行する。

    対応ベンダーが増えた場合も、この関数のロジックは変更不要。
    VENDOR_HUB_MAP に新しいベンダーのHub URLを1行追加し、VENDOR_ID の値を
    切り替えるだけでよい（概要設計書 4.5節参照）。

    NOTE: Hub配下のWrite Agentが自身でgovernance_client.evaluate()を
    呼ぶ設計（junos_netconf_write_a2a_serverと同型）だが、そちらの
    REVIEW判定は「人間がDiff/Historyで承認済み」という前提に基づく。
    本関数を呼び出す前に、必ず _evaluate_autonomous_deploy() で
    interconnect側のgovernance評価を完了させておくこと。
    """
    hub_url = VENDOR_HUB_MAP.get(VENDOR_ID)
    if not hub_url:
        raise RuntimeError(f"未知の VENDOR_ID '{VENDOR_ID}'（VENDOR_HUB_MAP に未登録）")

    exec_result = await _post(f"{hub_url}/execute", {"query": query, "deploy": False})
    trace_id = exec_result.get("trace_id")
    if not trace_id:
        raise RuntimeError(f"Hub /execute が trace_id を返さなかった: {exec_result}")
    return await _post(f"{hub_url}/deploy/{trace_id}", {
        "device": {"ip": DEVICE_IP, "username": DEVICE_USERNAME, "password": DEVICE_PASSWORD},
    })


async def _deploy_via_vendor_hub_structured(task_type: str, params: Dict[str, Any]) -> dict:
    """
    a2a-junos-core 側の /deploy_structured（概要設計書5.6節）を、タスク種別
    1つにつき1回呼び出す。LLM+RAGを経由しない決定論的経路であるため、
    _deploy_via_vendor_hub() と異なり dry-run(/execute)→deploy(/deploy/
    {trace_id}) の2段階に分ける必要がない。final_xml機構がそもそも防ごう
    としていた「dry-run時とdeploy時でLLMが異なるXMLを再生成してしまう」
    という問題自体が、LLMを使わないこの経路には存在しないためである。

    【重要・実装依存】この関数は a2a-junos-core 側に /deploy_structured
    エンドポイントが実装されていることを前提とする。未実装の間は
    VENDOR_STRUCTURED_TASK_BUILDERS に対象ベンダーを登録しないこと
    （登録すると404エラーになる）。

    想定しているエンドポイント契約（a2a-junos-core側の実装者向け）:

        POST /deploy_structured
        request:
            {
              "task_type": str,   # governanceアクション分類・XML生成
                                   # テンプレート選択に使用
                                   # （_classify_governance_action() の
                                   # キーワード判定と整合させること）
              "params":    dict,  # タスク種別ごとの構造化パラメータ
                                   # （_build_structured_tasks_junos()参照）
              "device": {"ip": str, "port": str,
                         "username": str, "password": str},
            }
        response:
            既存の /deploy/{trace_id} と同じレスポンス形状
            （status / diff / message 等。response_schema.is_ok() で
            成否判定できる形を維持すること）。内部的には
            task_type→決定論的XML生成→governance評価→
            deploy_netconf（単発commit + compare rollback 0）→audit
            という既存パイプラインをそのまま通す想定（5.6節参照）。

    対応ベンダーが増えた場合も、この関数のロジック自体は変更不要。
    VENDOR_HUB_MAP に新しいベンダーのHub URLを追加し、そのベンダー側にも
    /deploy_structured を実装したうえで VENDOR_STRUCTURED_TASK_BUILDERS に
    登録すればよい。
    """
    hub_url = VENDOR_HUB_MAP.get(VENDOR_ID)
    if not hub_url:
        raise RuntimeError(f"未知の VENDOR_ID '{VENDOR_ID}'（VENDOR_HUB_MAP に未登録）")

    return await _post(f"{hub_url}/deploy_structured", {
        "task_type": task_type,
        "params":    params,
        "device": {"ip": DEVICE_IP, "port": "830",
                   "username": DEVICE_USERNAME, "password": DEVICE_PASSWORD},
    })


# ═══════════════════════════════════════════════════════════════════════════════
# governance評価（本エージェント固有の解釈）
# ═══════════════════════════════════════════════════════════════════════════════

async def _evaluate_autonomous_deploy(connection_id: str, feature_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    自動承認モードでのデプロイ可否を governance に問い合わせる。

    通常のvendor書き込み系（junos.edit_config.* 等）とは異なるアクション
    名前空間 "interconnect.autonomous_deploy.*" を使う。policy.yaml側で
    この名前空間には以下のようなルールを追加することを想定している。

        - id: "interconnect-autonomous-deploy-review"
          action_pattern: "interconnect.autonomous_deploy.*"
          effect: "REVIEW"
          reason: "人間の承認を経ない自動プロビジョニングのため要確認"

    Returns:
        {"proceed": bool, "state": str, "reason": str}
    """
    gov = get_governance_client()
    own_provider = _connections.get(connection_id, {}).get("provider", PROVIDER_ID)
    result = await gov.evaluate(
        action="interconnect.autonomous_deploy.l3config",
        trace_id=connection_id,
        agent_name="a2a_interconnect_core",
        # ★修正（①完結条件）: グローバル変数ではなく、この接続が実際に
        #   どのidentity（provider）として作られたかを記録から参照する。
        action_detail={"provider": own_provider, **feature_config},
    )

    if result["effect"] == "DENY":
        await gov.log_event({
            "event_type": "POLICY_EVAL",
            "trace_id": connection_id,
            "agent_name": "a2a_interconnect_core",
            "action": "interconnect.autonomous_deploy.l3config",
            "note": f"自動接続を governance が拒否: {result['reason']}",
        })
        return {"proceed": False, "state": "BLOCKED", "reason": result["reason"]}

    if result["effect"] == "REVIEW":
        # ★ junos_netconf_write と異なり、ここでは実行継続しない。
        #   人間承認が実在しないため、REVIEWは「共用UIでの承認待ち」に
        #   遷移させ、POST /connections/{id}/approve が呼ばれるまで待機する。
        await gov.log_event({
            "event_type": "REVIEW_NOTED",
            "trace_id": connection_id,
            "agent_name": "a2a_interconnect_core",
            "action": "interconnect.autonomous_deploy.l3config",
            "note": f"人間承認待ちに遷移: {result['reason']}",
        })
        return {"proceed": False, "state": "AWAITING_HUMAN_APPROVAL", "reason": result["reason"]}

    # PERMIT
    return {"proceed": True, "state": "DEPLOYING", "reason": result.get("reason", "")}


# ═══════════════════════════════════════════════════════════════════════════════
# ネゴシエーション本体
# ═══════════════════════════════════════════════════════════════════════════════

async def _run_deployment(connection_id: str) -> None:
    """governance評価 → デプロイ → ANTA post-check → 相手へNotify。"""
    conn = _connections[connection_id]

    # ★前回試行の残骸クリア: 再試行（人間承認後の再実行等）の際、前回の
    #   governance_reason / error が新しい試行結果と混同されないようにする。
    #   実際にこの残骸が get_negotiation_status のレスポンスに混入し、
    #   判定が効いているかどうかを紛らわしくした実機不具合を踏まえた修正。
    conn.pop("error", None)

    feature_config = conn["feature_config"]

    gov_decision = await _evaluate_autonomous_deploy(connection_id, feature_config)
    conn["state"] = gov_decision["state"]
    conn["governance_reason"] = gov_decision["reason"]

    if not gov_decision["proceed"]:
        logger.info(f"[{connection_id}] デプロイ保留: {conn['state']} ({gov_decision['reason']})")
        return

    await _execute_deployment(connection_id)


async def _execute_deployment(connection_id: str) -> None:
    """
    governance承認後（PERMITまたは人間承認後）の実デプロイ処理。

    VENDOR_ID が VENDOR_STRUCTURED_TASK_BUILDERS に登録済みなら、
    決定論的経路（/deploy_structured、概要設計書5.6節）を使う。
    未登録（例: 凍結中の a2a-ceos-core）の場合は、従来通りNL経路
    （/execute → /deploy/{trace_id}）にフォールバックする。
    どちらの経路も、成否判定（is_ok）・失敗時のstate遷移・Notify送信の
    ロジックは共通にしてある。
    """
    conn = _connections[connection_id]
    feature_config = conn["feature_config"]
    conn["state"] = "DEPLOYING"
    conn.pop("error", None)  # ★再試行時の残骸クリア（_run_deploymentと同じ理由）

    structured_builder = VENDOR_STRUCTURED_TASK_BUILDERS.get(VENDOR_ID)

    try:
        if structured_builder is not None:
            # ── 決定論的経路（概要設計書5.6節、/deploy_structured） ──────────
            tasks = structured_builder(feature_config)
            deploy_results = []
            failed_items = []

            for task in tasks:
                attempt_results = []
                for attempt in range(1, DEPLOY_MAX_ATTEMPTS + 1):
                    result = await _deploy_via_vendor_hub_structured(task["task_type"], task["params"])
                    attempt_results.append(result)
                    if is_ok(result):
                        break
                    logger.warning(
                        f"[{connection_id}] タスク失敗（{attempt}/{DEPLOY_MAX_ATTEMPTS}回目）: "
                        f"{task['task_type']}（{task.get('description', '')}）"
                    )
                    if attempt < DEPLOY_MAX_ATTEMPTS:
                        await asyncio.sleep(DEPLOY_RETRY_DELAY_SEC)

                final_result = attempt_results[-1]
                deploy_results.append(final_result)
                if not is_ok(final_result):
                    failed_items.append({
                        "task_type":   task["task_type"],
                        "description": task.get("description", ""),
                        "result":      final_result,
                        "attempts":    len(attempt_results),
                    })

            conn["deploy_results"] = deploy_results

            if failed_items:
                conn["state"] = "FAILED"
                conn["error"] = (
                    f"{len(failed_items)}/{len(tasks)} 件のタスクが失敗しました。"
                    "詳細は deploy_results / failed_tasks を参照してください。"
                )
                conn["failed_tasks"] = failed_items
                logger.error(f"[{connection_id}] デプロイ失敗: {len(failed_items)}/{len(tasks)}件失敗")
                return

        else:
            # ── NL経路（フォールバック。/deploy_structured 未実装のベンダー向け） ──
            commands = _build_write_commands(feature_config)
            deploy_results = []
            failed_commands = []

            for cmd in commands:
                attempt_results = []
                for attempt in range(1, DEPLOY_MAX_ATTEMPTS + 1):
                    result = await _deploy_via_vendor_hub(cmd)
                    attempt_results.append(result)
                    if is_ok(result):
                        break
                    logger.warning(
                        f"[{connection_id}] コマンド失敗（{attempt}/{DEPLOY_MAX_ATTEMPTS}回目）: {cmd!r}"
                    )
                    if attempt < DEPLOY_MAX_ATTEMPTS:
                        await asyncio.sleep(DEPLOY_RETRY_DELAY_SEC)

                final_result = attempt_results[-1]
                deploy_results.append(final_result)
                # ★修正（実機検証で発見したバグ）: 従来はループが最後まで
                #   例外なく回れば無条件で VERIFIED にしていたため、Hubから
                #   200 OKで「all_failure」（PolicyChecker拒否・NETCONFエラー等）
                #   が返ってきても成功扱いになっていた。response_schema.is_ok()
                #   で各コマンドの実際のステータスを確認するよう修正。
                if not is_ok(final_result):
                    failed_commands.append({
                        "query": cmd,
                        "result": final_result,
                        "attempts": len(attempt_results),
                    })

            conn["deploy_results"] = deploy_results

            if failed_commands:
                conn["state"] = "FAILED"
                conn["error"] = (
                    f"{len(failed_commands)}/{len(commands)} 件のコマンドが失敗しました。"
                    "詳細は deploy_results / failed_commands を参照してください。"
                )
                conn["failed_commands"] = failed_commands
                logger.error(f"[{connection_id}] デプロイ失敗: {len(failed_commands)}/{len(commands)}件失敗")
                return

        conn["state"] = "VERIFIED"
        peer_url = _resolve_peer_base_url(conn["peer_provider"])
        # ★修正（①完結条件）: URLパスの provider セグメントも、グローバル変数ではなく
        #   この接続自身が記録している自分のidentityを使う。
        own_provider = conn.get("provider", PROVIDER_ID)
        await _post(f"{peer_url}/providers/{own_provider}/connections/{connection_id}/notify", {
            "connection_id": connection_id,
            "state": "VERIFIED",
        })
        logger.info(f"[{connection_id}] デプロイ完了・相手へNotify送信")

    except Exception as e:
        conn["state"] = "FAILED"
        conn["error"] = str(e)
        logger.error(f"[{connection_id}] デプロイ失敗: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 内部ハンドラ（REST エンドポイントとA2A Executorの両方から呼ばれる共通ロジック）
# ═══════════════════════════════════════════════════════════════════════════════

async def _do_create_connection(provider: str, environment: str, bandwidth_mbps: int, peer_provider: str) -> dict:
    """
    Negotiator側: ActivationKeyを発行して顧客（エージェント）に返す。

    ✅ 仕様準拠フェーズB（5.13節）: 従来はここでNegotiatorが自分から対向
    （Responder）へ`/confirm`を自動送信していたが、これは仕様の
    「顧客がActivationKeyを持ってResponder側へ行き、AcceptConnectionを
    呼ぶ」という中間ステップを省略し、かつ呼び出し方向（本来はResponderが
    Negotiatorへ確認する）も逆転させていた（5.12節で発見した乖離#1）。

    Equinix Fabric One（2026年9月発表）の「natural-language prompts」型
    入力を参考に、この中間ステップを自然言語エージェント（顧客役）が
    明示的に担う設計に変更した。本関数はActivationKeyを発行するだけで
    Responderには一切接続しない。顧客（エージェント）がこのActivationKey
    を`accept_connection_nl`skillでResponder側に持ち込むことで、初めて
    ネゴシエーションが先に進む。
    """
    connection_id = str(uuid.uuid4())
    activation_key = uuid.uuid4().hex

    _connections[connection_id] = {
        "connection_id": connection_id,
        "state": "PENDING",
        "provider": provider,
        "environment": environment,
        "bandwidth_mbps": bandwidth_mbps,
        "activation_key": activation_key,
        "peer_provider": peer_provider,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    return make_response(
        status=STATUS_SUCCESS, route="interconnect.create_connection",
        routed_to="", summary=(
            f"ActivationKeyを発行しました。このキーを持って{peer_provider}側に "
            f"accept_connection_nl を呼び出してください。"
        ),
        result=_connections[connection_id], trace_id=connection_id,
    )



async def _do_accept_connection_nl(
    provider: str, activation_key: str, connection_id: str,
    negotiator_provider: str, text: str = "",
) -> dict:
    """
    Responder側: 顧客（エージェント）が持ち込んだActivationKeyで接続を
    受け入れる（仕様のAcceptConnection相当、Fabric One流の自然言語入力）。

    ✅ 仕様準拠フェーズB（5.13節）:
    従来は`_do_confirm_activation_key`が「Negotiatorからの直接送信を
    無条件で信頼する」実装になっており、Bが独立してAに真正性を確認する
    手続きが存在しなかった。ここでは、まずローカルに仮登録した上で、
    Negotiator(A)へ`verify_activation_key`を呼んで検証し（B→A方向、
    仕様通り）、検証OKなら続けてAへ`GenerateFeatureGuidance`を要求する
    …のではなく、仕様のNegotiator主導の設計（Negotiatorがguidanceを
    要求する）に合わせ、検証OK後はAからのgenerate_feature_guidance
    呼び出しを待つ形にする（本関数ではAへの検証依頼のみ行う）。
    """
    peer_url = _resolve_peer_base_url(negotiator_provider)

    _connections[connection_id] = {
        "connection_id": connection_id,
        "state": "PENDING_VERIFY",
        "provider": provider,
        "activation_key": activation_key,
        "peer_provider": negotiator_provider,
        "accept_text": text,
    }

    try:
        verify_response = await _post(
            f"{peer_url}/providers/{negotiator_provider}/connections/{connection_id}/verify_activation_key",
            {
                "connection_id": connection_id,
                "activation_key": activation_key,
                "requesting_provider": provider,
            },
        )
    except Exception as e:
        _connections[connection_id]["state"] = "FAILED"
        return make_error_response(
            route="interconnect.accept_connection_nl", routed_to=peer_url,
            message=f"Negotiatorへの検証依頼に失敗: {e}", trace_id=connection_id,
        )

    if not verify_response.get("valid"):
        _connections[connection_id]["state"] = "FAILED"
        return make_error_response(
            route="interconnect.accept_connection_nl", routed_to=peer_url,
            message="ActivationKeyの検証に失敗しました（Negotiator側が無効と判定）",
            trace_id=connection_id,
        )

    _connections[connection_id]["state"] = "CONFIRMED"
    logger.info(f"[{connection_id}] ActivationKey検証成功。Negotiatorからのguidance要求待ち。")
    return make_response(
        status=STATUS_SUCCESS, route="interconnect.accept_connection_nl",
        routed_to=peer_url, summary="ActivationKeyを検証しました。Negotiatorからのguidance要求を待機します。",
        result=_connections[connection_id], trace_id=connection_id,
    )


async def _do_verify_activation_key(provider: str, connection_id: str, activation_key: str, requesting_provider: str) -> dict:
    """
    Negotiator側: Responderからの検証依頼を受ける（仕様のConfirmActivationKey相当、
    B→A方向）。検証成功後、自らResponderへGenerateFeatureGuidanceを要求し、
    範囲から値を選択してCreateFeatureへ進む一連の処理を非同期で開始する。
    """
    conn = _connections.get(connection_id)
    if conn is None:
        return {"valid": False, "reason": f"connection_id '{connection_id}' が見つかりません"}
    if conn.get("activation_key") != activation_key:
        return {"valid": False, "reason": "activation_keyが一致しません"}
    if conn.get("peer_provider") != requesting_provider:
        return {"valid": False, "reason": f"想定していた相手（'{conn.get('peer_provider')}'）と異なります"}

    conn["state"] = "CONFIRMED"
    logger.info(f"[{connection_id}] Responder（{requesting_provider}）からの検証依頼を確認、guidance要求フローを開始")
    asyncio.create_task(_negotiate_after_confirm(connection_id))
    return {"valid": True}


async def _negotiate_after_confirm(connection_id: str) -> None:
    """
    Negotiator側: 検証成功後、Responderへguidanceを要求→範囲内から値を選択→
    CreateFeatureへ進む一連の流れをバックグラウンドで実行する。
    """
    conn = _connections[connection_id]
    provider = conn["provider"]
    peer_provider = conn["peer_provider"]
    peer_url = _resolve_peer_base_url(peer_provider)

    try:
        guidance = await _post(
            f"{peer_url}/providers/{peer_provider}/connections/{connection_id}/generate_feature_guidance",
            {"connection_id": connection_id, "requesting_provider": provider},
        )
        conn["guidance"] = guidance
        conn["state"] = "GUIDANCE_ISSUED"

        proposed_config = _select_from_guidance(guidance, provider, peer_provider)
        logger.info(f"[{connection_id}] guidanceから値を選択: {proposed_config}")

        await _do_create_feature(provider, connection_id, proposed_config, propagate=True)
    except Exception as e:
        conn["state"] = "FAILED"
        conn["error"] = f"guidance要求〜Feature提案の途中で失敗: {e}"
        logger.error(f"[{connection_id}] {conn['error']}")


async def _do_generate_feature_guidance(provider: str, connection_id: str, requesting_provider: str) -> dict:
    """
    Responder側: Negotiatorからのguidance要求を受け、範囲（FeatureGuidance）を
    生成して返す（仕様のGenerateFeatureGuidance相当）。
    """
    conn = _connections.get(connection_id)
    if conn is None:
        raise KeyError(f"connection_id '{connection_id}' が見つかりません")
    if conn.get("peer_provider") != requesting_provider:
        raise ValueError(f"想定していた相手（'{conn.get('peer_provider')}'）と異なるリクエストです")

    guidance = _allocator.generate_guidance(provider, requesting_provider)
    conn["guidance"] = guidance
    conn["state"] = "GUIDANCE_ISSUED"
    logger.info(f"[{connection_id}] FeatureGuidance生成: {guidance}")
    return guidance


async def _do_create_feature(provider: str, connection_id: str, proposed_config: Dict[str, Any], propagate: bool = True) -> dict:
    """
    Negotiator/Responder共通: FeatureConfigを確定させる。
    受理した側は governance評価 → デプロイを非同期でキックする。

    ★修正（実機検証で発見したバグ）: 従来はローカルの_connectionsを
    更新するだけで、対向プロセスへの転送処理が無かった。これにより、
    curlで最初に叩いた側（この検証ではceos/aws役）だけがデプロイされ、
    対向（junos/gcp役）は create_feature を一度も受け取らないまま
    state が GUIDANCE_ISSUED に取り残される不具合があった。

    propagate=True（人間/UIからの最初の呼び出し）の場合のみ対向へ転送し、
    対向側では propagate=False で受信して無限転送を防ぐ。

    ★追加修正（①完結、11.0節で発見した検証漏れ）: proposed_config が
    Responder提示のguidanceと一致するかを確認せず無条件で受理していた。
    プロトコル上「Responderが提示した範囲内でのみFeatureを提案できる」
    はずの制約を、ここで強制する。
    """
    if connection_id not in _connections:
        raise KeyError(f"connection_id '{connection_id}' が見つかりません")

    conn = _connections[connection_id]
    guidance = conn.get("guidance")
    if guidance and "vlanRange" not in guidance:
        # ★仕様準拠フェーズB以前のguidance形式（確定値）が残っていた場合の
        #   後方互換チェック。新形式（範囲、vlanRangeキーを持つ）の場合は
        #   _select_from_guidance() が既に範囲内に収まる値しか生成しない
        #   ため、ここでの一致比較は行わない（範囲 vs 確定値は構造が
        #   異なり単純比較できないため）。
        mismatched = {
            k: (guidance[k], proposed_config.get(k))
            for k in guidance
            if k in proposed_config and proposed_config[k] != guidance[k]
        }
        if mismatched:
            detail = "; ".join(f"{k}: guidance={g!r} proposed={p!r}" for k, (g, p) in mismatched.items())
            logger.warning(f"[{connection_id}] guidance不一致のためcreate_featureを拒否: {detail}")
            return make_error_response(
                route="interconnect.create_feature", routed_to="",
                message=f"proposed_configがguidanceと一致しません（{detail}）。"
                        "guidanceで提示された値をそのまま使用してください。",
                trace_id=connection_id,
            )
    elif guidance:
        # ★新形式（範囲）の場合の検証: 提案値が範囲内に収まっているかを確認する。
        vlan_range = guidance.get("vlanRange", {})
        if "vlanId" in proposed_config and vlan_range:
            if not (vlan_range["start"] <= proposed_config["vlanId"] <= vlan_range["end"]):
                return make_error_response(
                    route="interconnect.create_feature", routed_to="",
                    message=f"proposed_configのvlanId（{proposed_config['vlanId']}）が"
                            f"guidanceの範囲（{vlan_range['start']}〜{vlan_range['end']}）外です。",
                    trace_id=connection_id,
                )
        mtu_range = guidance.get("mtuRange", {})
        if "mtuBytes" in proposed_config and mtu_range:
            if not (mtu_range["start"] <= proposed_config["mtuBytes"] <= mtu_range["end"]):
                return make_error_response(
                    route="interconnect.create_feature", routed_to="",
                    message=f"proposed_configのmtuBytes（{proposed_config['mtuBytes']}）が"
                            f"guidanceの範囲（{mtu_range['start']}〜{mtu_range['end']}）外です。",
                    trace_id=connection_id,
                )

    conn["feature_config"] = proposed_config
    conn["state"] = "FEATURE_ACCEPTED"

    asyncio.create_task(_run_deployment(connection_id))

    if propagate:
        peer_provider = conn.get("peer_provider", "unknown")
        asyncio.create_task(_forward_create_feature(provider, peer_provider, connection_id, proposed_config))

    return make_response(
        status=STATUS_SUCCESS, route="interconnect.create_feature",
        routed_to="", summary="Feature合意・自動デプロイフローを開始しました",
        result=conn, trace_id=connection_id,
    )


async def _forward_create_feature(provider: str, peer_provider: str, connection_id: str, proposed_config: Dict[str, Any]) -> None:
    """CreateFeatureを対向プロセスへ転送し、対向側でも独立してデプロイを開始させる。"""
    peer_url = _resolve_peer_base_url(peer_provider)
    try:
        await _post(f"{peer_url}/providers/{peer_provider}/connections/{connection_id}/features", {
            "connection_id": connection_id,
            "proposed_config": proposed_config,
            "propagate": False,
        })
        logger.info(f"[{connection_id}] CreateFeatureを対向({peer_url})へ転送完了")
    except Exception as e:
        logger.error(f"[{connection_id}] CreateFeatureの対向への転送に失敗: {e}")


async def _do_notify_connection_status(provider: str, connection_id: str, state: str) -> dict:
    if connection_id in _connections:
        _connections[connection_id]["peer_state"] = state
    return {"received": True}


async def _do_approve(provider: str, connection_id: str, operator_note: Optional[str] = None) -> dict:
    """
    共用UIの「Interconnect」タブから、人間がAWAITING_HUMAN_APPROVALの
    接続を承認する処理。governance REVIEW判定を経て保留中の接続のみを
    実行に進める。
    """
    conn = _connections.get(connection_id)
    if not conn:
        raise KeyError(f"connection_id '{connection_id}' が見つかりません")
    if conn["state"] != "AWAITING_HUMAN_APPROVAL":
        raise ValueError(f"現在の状態 '{conn['state']}' は承認待ちではありません")

    gov = get_governance_client()
    await gov.log_event({
        "event_type": "OPERATOR_APPROVE",
        "trace_id": connection_id,
        "agent_name": "a2a_interconnect",
        "note": f"共用UIから人間が承認: {operator_note or ''}",
    })

    asyncio.create_task(_execute_deployment(connection_id))
    return {"status": "approving", "connection_id": connection_id}


async def _do_get_status(provider: str, connection_id: str) -> dict:
    """共用UIの「Interconnect」タブがポーリングする状態取得。"""
    conn = _connections.get(connection_id)
    if not conn:
        raise KeyError(f"connection_id '{connection_id}' が見つかりません")
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# REST エンドポイント（Connection Coordinator API のサブセット・薄いラッパー）
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/healthz")
async def healthz():
    return {"status": "ok", "provider": PROVIDER_ID, "port": A2A_PORT, "version": VERSION}


@app.post(f"/providers/{{provider}}/connections")
async def create_connection(provider: str, req: CreateConnectionRequest):
    return await _do_create_connection(provider, req.environment, req.bandwidth_mbps, req.peer_provider)


@app.post(f"/providers/{{provider}}/connections/{{connection_id}}/accept_connection_nl")
async def accept_connection_nl(provider: str, connection_id: str, req: AcceptConnectionRequest):
    return await _do_accept_connection_nl(
        provider, req.activation_key, connection_id, req.negotiator_provider, req.text,
    )


@app.post(f"/providers/{{provider}}/connections/{{connection_id}}/verify_activation_key")
async def verify_activation_key(provider: str, connection_id: str, req: VerifyActivationKeyRequest):
    return await _do_verify_activation_key(provider, connection_id, req.activation_key, req.requesting_provider)


@app.post(f"/providers/{{provider}}/connections/{{connection_id}}/generate_feature_guidance")
async def generate_feature_guidance(provider: str, connection_id: str, req: GenerateFeatureGuidanceRequest):
    try:
        return await _do_generate_feature_guidance(provider, connection_id, req.requesting_provider)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(409, str(e))


@app.post(f"/providers/{{provider}}/connections/{{connection_id}}/features")
async def create_feature(provider: str, connection_id: str, req: CreateFeatureRequest):
    try:
        return await _do_create_feature(provider, connection_id, req.proposed_config, req.propagate)
    except KeyError as e:
        raise HTTPException(404, str(e))


@app.post(f"/providers/{{provider}}/connections/{{connection_id}}/notify")
async def notify_connection_status(provider: str, connection_id: str, req: NotifyConnectionStatusRequest):
    return await _do_notify_connection_status(provider, connection_id, req.state)


@app.post(f"/providers/{{provider}}/connections/{{connection_id}}/approve")
async def approve_pending_connection(provider: str, connection_id: str, req: ApproveRequest):
    try:
        return await _do_approve(provider, connection_id, req.operator_note)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(409, str(e))


@app.get(f"/providers/{{provider}}/connections/{{connection_id}}")
async def get_negotiation_status(provider: str, connection_id: str):
    try:
        return await _do_get_status(provider, connection_id)
    except KeyError as e:
        raise HTTPException(404, str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# ①意図抽出（4.3節）：自然言語 → LLM（軽量） → 構造化パラメータ
# ─────────────────────────────────────────────────────────────────────────────
# Equinix Fabric One（2026年9月発表）が謳う「agent-based requests or
# natural-language prompts」という入力方式を、a2a-interconnect側でも
# 受け付けられるようにする。
#
# 重要な設計原則（4.3節から変更なし）: LLMが担うのはこの抽出フェーズのみ。
# 抽出結果は _do_create_connection() にそのまま渡り、以降のプロトコル交渉
# （ActivationKey発行・FeatureGuidance生成・L3パラメータ採番）は
# 一切LLMを経由しない決定論的ロジックのまま変更しない
# （VLAN/ASN/Subnetの割当てにハルシネーションのリスクを持ち込まないため）。
# ═══════════════════════════════════════════════════════════════════════════════

_INTENT_EXTRACTION_PROMPT = """あなたはInterconnect接続要求から、以下3つのパラメータだけを抽出するアシスタントです。

抽出するフィールド:
  - environment: 接続先環境の識別子（例: "us-east-1"）。地域名やリージョン名から抽出する。
  - bandwidth_mbps: 帯域幅（Mbps単位の整数）。「1Gbps」なら1000、「500Mbps」なら500に変換する。
  - peer_provider: 接続相手のプロバイダID。"aws"「gcp」「azure」のいずれかの小文字英字に正規化する。

出力形式: 上記3フィールドのみを持つJSONオブジェクト1つだけを出力すること。
説明文・Markdownのバッククォート・前置きは一切含めないこと。
3つのフィールドのいずれかが文章から特定できない場合は、そのフィールドの値をnullにすること
（推測や補完は絶対にしないこと。誤った値を入れるくらいなら不明のままにする）。

入力: {text}
出力:"""


def _extract_create_connection_intent(text: str) -> Dict[str, Any]:
    """
    自然言語からcreate_connectionのパラメータを抽出する（意図抽出フェーズのみ）。

    Returns:
        {"environment": str|None, "bandwidth_mbps": int|None, "peer_provider": str|None}

    Raises:
        ValueError: LLM出力がJSONとしてパースできない場合
    """
    llm = build_llm_with_fallback()
    prompt = _INTENT_EXTRACTION_PROMPT.format(text=text)
    raw = llm.invoke(prompt)
    raw_text = raw.content if hasattr(raw, "content") else str(raw)

    # Markdownのバッククォート等が混入しても最低限救えるようにする
    cleaned = raw_text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()

    try:
        extracted = json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLMの抽出結果がJSONとして解析できません: {e}（生出力: {raw_text!r}）")

    return {
        "environment": extracted.get("environment"),
        "bandwidth_mbps": extracted.get("bandwidth_mbps"),
        "peer_provider": extracted.get("peer_provider"),
    }


async def _do_create_connection_nl(provider: str, natural_language_text: str) -> dict:
    """
    ①意図抽出 → ②既存の _do_create_connection()（決定論的）という2段階を
    1つのskillとして提供する。抽出に失敗・不足がある場合は、ここで
    エラーを返して停止する（不明な値のまま交渉に進ませない）。
    """
    try:
        intent = _extract_create_connection_intent(natural_language_text)
    except ValueError as e:
        return make_error_response(
            route="interconnect.create_connection_nl", routed_to="",
            message=f"自然言語からの意図抽出に失敗しました: {e}", trace_id="",
        )

    missing = [k for k, v in intent.items() if v is None]
    if missing:
        return make_error_response(
            route="interconnect.create_connection_nl", routed_to="",
            message=(
                f"以下のパラメータが文章から特定できませんでした: {', '.join(missing)}。"
                "具体的な値（環境名・帯域・接続先プロバイダ）を含めて再度入力してください。"
            ),
            trace_id="",
        )

    logger.info(f"[create_connection_nl] 抽出結果: {intent}")
    return await _do_create_connection(
        provider, intent["environment"], intent["bandwidth_mbps"], intent["peer_provider"],
    )


# ═══════════════════════════════════════════════════════════════════════════════
# A2A プロトコル面（splunk_a2a_bridge.py と同型のAgentExecutorパターン）
# ─────────────────────────────────────────────────────────────────────────────
# message/send のテキストパートに JSON ペイロード
#   {"skill": "create_connection", "environment": "...", ...}
# を積んで送ると、同名の内部 _do_* 関数へディスパッチする
# （governance_a2a_server.py の message/send 経由スキル呼び出しと同じ規約）。
# ═══════════════════════════════════════════════════════════════════════════════

_SKILL_DISPATCH = {
    "create_connection":        lambda p: _do_create_connection(
        p.get("provider", PROVIDER_ID), p["environment"], p["bandwidth_mbps"], p["peer_provider"]),
    "create_connection_nl":     lambda p: _do_create_connection_nl(
        p.get("provider", PROVIDER_ID), p["text"]),
    "accept_connection_nl":     lambda p: _do_accept_connection_nl(
        p.get("provider", PROVIDER_ID), p["activation_key"], p["connection_id"],
        p["negotiator_provider"], p.get("text", "")),
    "verify_activation_key":    lambda p: _do_verify_activation_key(
        p.get("provider", PROVIDER_ID), p["connection_id"], p["activation_key"], p["requesting_provider"]),
    "generate_feature_guidance": lambda p: _do_generate_feature_guidance(
        p.get("provider", PROVIDER_ID), p["connection_id"], p["requesting_provider"]),
    "create_feature":           lambda p: _do_create_feature(
        p.get("provider", PROVIDER_ID), p["connection_id"], p["proposed_config"], p.get("propagate", True)),
    "notify_connection_status": lambda p: _do_notify_connection_status(
        p.get("provider", PROVIDER_ID), p["connection_id"], p["state"]),
    "approve":                  lambda p: _do_approve(
        p.get("provider", PROVIDER_ID), p["connection_id"], p.get("operator_note")),
    "get_negotiation_status":   lambda p: _do_get_status(
        p.get("provider", PROVIDER_ID), p["connection_id"]),
}


class InterconnectBridgeExecutor(AgentExecutor):
    """
    A2Aリクエストを受け取り、Connection Coordinator API の対応するスキルへ
    ディスパッチする（splunk_a2a_bridge.py の SplunkBridgeExecutor と同型）。
    """

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        raw = (context.get_user_input() or "").strip()
        if not raw:
            await event_queue.enqueue_event(
                new_text_message("JSON形式のペイロードが空です。"
                                 '例: {"skill": "get_negotiation_status", "connection_id": "..."}\n'
                                 '自然言語での接続要求は create_connection_nl skill を使用: '
                                 '{"skill": "create_connection_nl", "text": "AWSのus-east-1とGCPの間に1Gbpsの接続を作って"}')
            )
            return

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            await event_queue.enqueue_event(new_text_message(f"JSONの解析に失敗しました: {e}"))
            return

        skill = payload.get("skill")
        handler = _SKILL_DISPATCH.get(skill)
        if handler is None:
            await event_queue.enqueue_event(new_text_message(
                f"未知の skill '{skill}' です。利用可能: {', '.join(_SKILL_DISPATCH)}"
            ))
            return

        try:
            result = await handler(payload)
        except (KeyError, ValueError) as e:
            result = {"status": STATUS_ERROR, "message": str(e)}
        except Exception as e:
            logger.exception(f"skill '{skill}' 実行エラー")
            result = {"status": STATUS_ERROR, "message": str(e)}

        await event_queue.enqueue_event(new_text_message(json.dumps(result, ensure_ascii=False)))

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        raise NotImplementedError("このエージェントはタスクのキャンセルに対応していません")


def _build_agent_card() -> AgentCard:
    skills = [
        AgentSkill(
            id="create_connection",
            name="CreateConnection",
            description="ActivationKeyを発行し、対向Providerへ接続要求を転送する（Negotiator側）。",
            tags=["interconnect", "negotiation"],
            examples=['{"skill":"create_connection","environment":"us-east-1","bandwidth_mbps":1000,"peer_provider":"gcp"}'],
        ),
        AgentSkill(
            id="create_connection_nl",
            name="CreateConnectionFromNaturalLanguage",
            description=(
                "自然言語の接続要求からenvironment/bandwidth_mbps/peer_providerをLLMで抽出し、"
                "create_connectionへ橋渡しする（Fabric One等のnatural-language prompts型の"
                "入力を受け付けるための窓口）。抽出後のプロトコル交渉自体は決定論的処理のまま。"
            ),
            tags=["interconnect", "negotiation", "nl"],
            examples=['{"skill":"create_connection_nl","text":"AWSのus-east-1とGCPの間に1Gbpsの接続を作って"}'],
        ),
        AgentSkill(
            id="accept_connection_nl",
            name="AcceptConnectionFromNaturalLanguage",
            description=(
                "顧客（エージェント）がActivationKeyを持ち込み、Responder側で接続を受け入れる"
                "（仕様のAcceptConnection相当、Fabric One流の自然言語入力）。"
                "activation_key等の重要な値はLLM抽出させず構造化パラメータで受け取る。"
            ),
            tags=["interconnect", "negotiation", "nl"],
            examples=['{"skill":"accept_connection_nl","text":"GCP側で接続を受け入れて","activation_key":"...","connection_id":"...","negotiator_provider":"aws"}'],
        ),
        AgentSkill(
            id="verify_activation_key",
            name="VerifyActivationKey",
            description=(
                "Responderからの検証依頼を受け、ActivationKeyの真正性を確認する"
                "（仕様のConfirmActivationKey相当、B→A方向）。検証成功後、"
                "自らGenerateFeatureGuidanceを要求する一連の処理を開始する。"
            ),
            tags=["interconnect", "negotiation"],
        ),
        AgentSkill(
            id="generate_feature_guidance",
            name="GenerateFeatureGuidance",
            description=(
                "Negotiatorからのguidance要求を受け、VLAN/ASN/Subnet/MTUの範囲"
                "（FeatureGuidance）を生成して返す（仕様準拠、確定値ではなく範囲）。"
            ),
            tags=["interconnect", "negotiation"],
        ),
        AgentSkill(
            id="create_feature",
            name="CreateFeature",
            description="L3BaseConfig（VLAN/ASN/Subnet/MTU）を確定させ、自動デプロイフローを開始する。",
            tags=["interconnect", "provisioning"],
        ),
        AgentSkill(
            id="notify_connection_status",
            name="NotifyConnectionStatus",
            description="有効化完了（VERIFIED等）を相互に通知する。",
            tags=["interconnect", "lifecycle"],
        ),
        AgentSkill(
            id="approve",
            name="Approve",
            description="governanceがREVIEW判定した接続を、人間が共用UIから承認する。",
            tags=["interconnect", "governance"],
        ),
        AgentSkill(
            id="get_negotiation_status",
            name="GetNegotiationStatus",
            description="指定したconnection_idの現在の交渉状態を取得する（UI向けポーリング用）。",
            tags=["interconnect", "status"],
            examples=['{"skill":"get_negotiation_status","connection_id":"..."}'],
        ),
    ]
    interface = AgentInterface(
        url=f"{A2A_PUBLIC_URL}/",
        protocol_binding=TransportProtocol.JSONRPC,
        protocol_version="1.0",
    )
    return AgentCard(
        name=f"a2a-interconnect ({PROVIDER_ID})",
        description=(
            "Connection Coordinator API（OpenAPI 3.0 Interconnect）をA2Aプロトコルで"
            "公開するIntegration Layerのブリッジエージェント。ベンダー固有の設定投入は"
            "既存のVendor Core Layer（a2a-ceos-core等）へ委譲する。"
        ),
        version=VERSION,
        supported_interfaces=[interface],
        default_input_modes=["text"],
        default_output_modes=["text"],
        capabilities=AgentCapabilities(streaming=False),
        skills=skills,
    )


def main():
    agent_card = _build_agent_card()
    request_handler = DefaultRequestHandler(
        agent_executor=InterconnectBridgeExecutor(),
        task_store=InMemoryTaskStore(),
        agent_card=agent_card,
    )

    # ★ enable_v0_3_compat=True は必須。a2a-governance README で指摘されている通り、
    #   これを付けないと message/send 等のv0.3形式メソッド名が -32601 Method not found
    #   （HTTPステータスは200のまま）を返し、静かにフォールバックへ落ちる。
    add_a2a_routes_to_fastapi(
        app,
        agent_card_routes=create_agent_card_routes(agent_card),
        jsonrpc_routes=create_jsonrpc_routes(request_handler, rpc_url="/", enable_v0_3_compat=True),
        rest_routes=create_rest_routes(request_handler),
    )

    logger.info("=" * 64)
    logger.info(f"a2a-interconnect v{VERSION} 起動 (PROVIDER_ID={PROVIDER_ID})")
    logger.info("=" * 64)
    logger.info(f"  Port           : {A2A_PORT}")
    logger.info(f"  Agent Card     : {A2A_PUBLIC_URL}/.well-known/agent-card.json")
    logger.info(f"  A2A Endpoint   : {A2A_PUBLIC_URL}/  (message/send, enable_v0_3_compat=True)")
    logger.info(f"  Peer Map       : {PEER_BASE_URL_MAP}")
    logger.info(f"  Vendor Hub Map : {VENDOR_HUB_MAP} (現在: {VENDOR_ID})")
    _structured = VENDOR_ID in VENDOR_STRUCTURED_TASK_BUILDERS
    logger.info(
        f"  デプロイ経路   : "
        f"{'決定論的（/deploy_structured）' if _structured else 'NL経路（/execute + /deploy/{trace_id}、フォールバック）'}"
    )
    logger.info(f"  Device IP      : {DEVICE_IP}")
    logger.info(f"  Governance     : {os.getenv('GOVERNANCE_A2A_URL', 'http://localhost:8190')}")
    logger.info("=" * 64)
    uvicorn.run(app, host=A2A_HOST, port=A2A_PORT, log_level="info")


if __name__ == "__main__":
    main()
