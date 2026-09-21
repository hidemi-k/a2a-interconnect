#!/usr/bin/env python3
# Copyright (c) 2026 hidemi-k
# Licensed under the MIT License.
"""
governance_client.py — governance_a2a_server クライアント
==========================================================
各エージェント（xdp / netconf_rag / eapi_config 等）が
governance_a2a_server (:8190) を呼び出すための共通クライアント。

【フォールバック設計（fail-open / fail-closed の使い分け）】

  書き込み系アクション → fail-closed
    governance サーバが応答しない場合は DENY 扱いにする。
    「ガバナンスを通らずに設定変更が実行される」パスを閉じる。
    対象: netconf.edit_config.* / eapi.config.* / xdp.block.* /
          ztna.lock.* / containment.acl.* 等

  読み取り系アクション → fail-open
    governance サーバが応答しない場合は PERMIT を返す。
    参照系が止まってもシステムの運用継続性を損なわない。
    対象: eapi.show.* / anta.* / diagnose.* / xdp.stats.* 等

【使い方】
  from governance_client import GovernanceClient

  _gov = GovernanceClient()

  result = await _gov.evaluate(
      action       = "netconf.edit_config.vlan",
      trace_id     = trace_id,
      agent_name   = "netconf_rag",
      action_detail= {"xml": xml_str[:200]},
  )

  if result["effect"] == "DENY":
      return {"status": "blocked", "reason": result["reason"]}
  if result["effect"] == "REVIEW":
      return {"status": "review_required", "request_id": result["request_id"]}
  # PERMIT → 実行継続
"""

import logging
import os
import uuid
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger("governance_client")

GOVERNANCE_A2A_URL = os.getenv("GOVERNANCE_A2A_URL", "http://localhost:8190")

_EVAL_TIMEOUT  = float(os.getenv("GOVERNANCE_EVAL_TIMEOUT",  "3.0"))
_AUDIT_TIMEOUT = float(os.getenv("GOVERNANCE_AUDIT_TIMEOUT", "5.0"))

# ── アクション種別の分類 ────────────────────────────────────────────────────
_WRITE_PREFIXES = (
    "netconf.edit_config.",
    "eapi.config.",
    "xdp.block.",
    "xdp.unblock.",
    "xdp.qos_set.",
    "xdp.load_program.",
    "ztna.drop_block.",
    "ztna.lock.",
    "ztna.system_lock",
    "containment.acl.",
    "containment.global_lock",
)


def _is_write_action(action: str) -> bool:
    """書き込み系アクション（fail-closed 対象）かどうかを判定する。"""
    return any(action.startswith(p) for p in _WRITE_PREFIXES)


class GovernanceClient:
    """
    governance_a2a_server への非同期クライアント。

    フォールバック設計:
      書き込み系（fail-closed）: governance 未応答時は DENY を返す
      読み取り系（fail-open）: governance 未応答時は PERMIT を返す
    """

    def __init__(self, base_url: str = GOVERNANCE_A2A_URL):
        self._base_url = base_url.rstrip("/")
        logger.info(f"[GovernanceClient] 接続先: {self._base_url}")

    async def evaluate(
        self,
        action:        str,
        trace_id:      str                      = "",
        agent_name:    str                      = "unknown",
        action_detail: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        governance_a2a の REST /evaluate を呼び出す。

        Returns:
            {
                "effect":     "PERMIT" | "DENY" | "REVIEW",
                "rule_id":    str | None,
                "reason":     str,
                "request_id": str,     # REVIEW の場合のみ
                "latency_ms": float,
                "_fallback":  bool,    # governance 未応答時のフォールバック
                "_fail_mode": "closed" | "open",
            }
        """
        is_write = _is_write_action(action)

        # ★修正: trace_id未指定（空文字）のまま送ると、監査ログに
        #   "trace_id": "" のエントリが溜まり続け、Splunk AO等のOTel trace_idとの
        #   相関が一切取れなくなる。ここで32桁hex（OTelのtrace_id形式と同一の桁数）
        #   を自動生成し、警告ログで気付けるようにする。
        if not trace_id:
            trace_id = uuid.uuid4().hex
            logger.warning(
                f"[GovernanceClient] trace_id が未指定のため自動生成しました: "
                f"{trace_id}（呼び出し元で trace_id を明示的に渡すことを推奨します）"
            )

        payload = {
            "action":        action,
            "trace_id":      trace_id,
            "agent_name":    agent_name,
            "action_detail": action_detail,
        }

        try:
            async with httpx.AsyncClient(timeout=_EVAL_TIMEOUT) as client:
                # governance_a2a_server の REST /evaluate に直接POSTする。
                # （A2A message/send 経由は -32601 Method not found を返し、
                #   HTTPステータスは200のため静かにフォールバックへ落ちるバグが
                #   実機検証で判明したため、REST直呼びに変更した。）
                resp = await client.post(f"{self._base_url}/evaluate", json=payload)
                resp.raise_for_status()
                result = resp.json()
                # ★修正: 以前は `if result:` で真偽値チェックしていたため、
                #   サーバが200で空dict等のfalsy値を返した場合、例外を投げずに
                #   下のフォールバック処理へ静かに流れ込み、ログが一切残らない
                #   「静かな失敗」経路になっていた（実機検証で判明した過去のバグと
                #   同種のパターン）。resp.raise_for_status()を通過した時点で
                #   HTTP的には成功なので、内容の真偽値で判断せず常に返す。
                if not isinstance(result, dict) or "effect" not in result:
                    logger.warning(
                        f"[GovernanceClient] governance サーバから不正な応答形式 "
                        f"（'effect'キー無し）: {result!r} — フォールバックへ切替"
                    )
                else:
                    return result

        except httpx.ConnectError:
            logger.warning(
                f"[GovernanceClient] 接続失敗 ({self._base_url}) "
                f"— {'DENY (fail-closed)' if is_write else 'PERMIT (fail-open)'} にフォールバック"
            )
        except httpx.TimeoutException:
            logger.warning(
                f"[GovernanceClient] タイムアウト ({_EVAL_TIMEOUT}s) "
                f"— {'DENY (fail-closed)' if is_write else 'PERMIT (fail-open)'} にフォールバック"
            )
        except Exception as e:
            logger.warning(
                f"[GovernanceClient] evaluate エラー: {e} "
                f"— {'DENY' if is_write else 'PERMIT'} にフォールバック"
            )

        # ── フォールバック ──────────────────────────────────────────────────
        if is_write:
            return {
                "effect":     "DENY",
                "rule_id":    None,
                "reason":     (
                    f"governance サーバ（{self._base_url}）に到達できません。"
                    "安全のため書き込み操作を拒否しました（fail-closed）。"
                    "governance_a2a_server の起動状態を確認してください。"
                ),
                "latency_ms": 0.0,
                "_fallback":  True,
                "_fail_mode": "closed",
            }
        else:
            return {
                "effect":     "PERMIT",
                "rule_id":    None,
                "reason":     "governance サーバ未応答（読み取り系のため fail-open）",
                "latency_ms": 0.0,
                "_fallback":  True,
                "_fail_mode": "open",
            }

    async def evaluate_with_current_trace(
        self,
        action:        str,
        agent_name:    str                      = "unknown",
        action_detail: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        evaluate() のtrace_id引数を、現在のOTelスパンから自動取得する版。

        OpenTelemetryでA2A呼び出しが計装されている場合、そのスパンの
        trace_id（32桁hex）をそのままgovernanceのtrace_idとして使うことで、
        Splunk AOのTrace Explorer上のtrace_idと監査ログ（JSONL）のtrace_idが
        完全一致し、追加の相関テーブルなしで trace_id 一本で両者を突き合わせられる。

        opentelemetryが未インストール、または有効なスパンが無い環境でも
        例外を出さず、trace_idが空のままevaluate()にフォールバックする
        （evaluate()側で自動生成される）。
        """
        trace_id = ""
        try:
            from opentelemetry import trace as _otel_trace
            span_ctx = _otel_trace.get_current_span().get_span_context()
            if span_ctx.is_valid:
                trace_id = format(span_ctx.trace_id, "032x")
        except ImportError:
            logger.debug(
                "[GovernanceClient] opentelemetry未インストールのため "
                "trace_id自動取得をスキップします"
            )
        except Exception as e:
            logger.debug(f"[GovernanceClient] OTel trace_id取得エラー（無視）: {e}")

        return await self.evaluate(
            action=action, trace_id=trace_id,
            agent_name=agent_name, action_detail=action_detail,
        )

    async def log_event(self, entry: Dict[str, Any]) -> bool:
        """監査ログを governance に記録する（fire-and-forget）。"""
        try:
            async with httpx.AsyncClient(timeout=_AUDIT_TIMEOUT) as client:
                resp = await client.post(
                    f"{self._base_url}/audit/event",
                    json=entry,
                )
                return resp.status_code == 200
        except Exception as e:
            logger.debug(f"[GovernanceClient] log_event 失敗（無視）: {e}")
            return False

    async def is_permitted(self, action: str, **kwargs) -> bool:
        """PERMIT かどうかだけを返す簡易版。"""
        result = await self.evaluate(action, **kwargs)
        return result["effect"] == "PERMIT"

    async def evaluate_and_log(
        self,
        action:        str,
        trace_id:      str,
        agent_name:    str,
        action_detail: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """evaluate を呼び、TASK_START として audit にも記録する。"""
        result = await self.evaluate(
            action=action, trace_id=trace_id,
            agent_name=agent_name, action_detail=action_detail,
        )
        await self.log_event({
            "event_type":    "TASK_START",
            "trace_id":      trace_id,
            "agent_name":    agent_name,
            "action":        action,
            "action_detail": action_detail,
            "policy_effect": result["effect"],
            "rule_id":       result["rule_id"],
            "_fallback":     result.get("_fallback", False),
            "_fail_mode":    result.get("_fail_mode", ""),
        })
        return result


_default_client: Optional[GovernanceClient] = None


def get_governance_client() -> GovernanceClient:
    global _default_client
    if _default_client is None:
        _default_client = GovernanceClient()
    return _default_client
