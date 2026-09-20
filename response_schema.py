#!/usr/bin/env python3
# Copyright (c) 2026 hidemi-k
# Licensed under the MIT License.
"""
core/response_schema.py — A2A Hub レスポンス共通スキーマ
=========================================================

【重要: 適用スコープ】
  このモジュールは「これから新規に書くHub」向けの規約ファイルである。
  既存の Arista Hub (task_decompose_a2a_server.py) および
  Junos Hub (junos_hub_a2a_server.py) には適用しない。

  理由:
    既存Hubのレスポンスフィールドは app_a2a.py が直接参照しており、
    フィールド名を変えると app_a2a.py 側も連鎖変更が必要になる。
    現時点でそのリスクを取る必要はない。

  新ベンダー（Cisco 等）のHubを新規に書くときは、
  このモジュールの make_response() / make_error_response() を使って
  フィールド名を最初から統一する。

【既存Hubのフィールド構造（参照用・変更しない）】
  result 直下（Hubが返す）:
    status, route, routed_to, summary, result（=inner）,
    query, message, is_read, overall_status,
    xml, task_summaries, tasks,
    eapi_cmds, eapi_diff, eapi_warning, session_diff,
    analysis, exec_tags, new_issues, snapshot_id,
    _artifact_anta_report, _artifact_xdp_log,
    _artifact_report, _artifact_diff

  inner（= result["result"]、各A2Aサーバが返す）:
    status, summary, overall_status, message,
    cmds, formatted, diff, warning,
    task_summaries, tasks, results,
    tests_total, tests_passed, tests_failed,
    engine, snapshot_id, new_issues, action,
    analysis, exec_tags, _raw_text

【将来フィールドの予約】
  新Hubでは以下のキーを追加する。
  既存Hubは今は返さないが、app_a2a.py 側で .get() で読む分には無害。
  将来 diagnose → 承認 → 実行 フローを実装するときに使う。

    uncertainty:                 str | None   — 診断の不確実性メモ
    human_confirmation_required: bool         — True のとき UI が承認ボタンを表示
    recommended_action:          str | None   — 是正コマンド候補

【使い方（新規Hubのみ）】
  from core.response_schema import make_response, make_error_response, is_ok

  # 成功時
  return make_response(
      status    = "success",
      route     = "write",
      routed_to = NETCONF_A2A_URL,
      summary   = "VLAN 100 を追加しました",
      result    = inner,         # ← 既存との互換のため "result" キーを維持
      trace_id  = trace_id,
  )

  # エラー時
  return make_error_response(
      route     = "write",
      routed_to = NETCONF_A2A_URL,
      message   = f"接続エラー: {e}",
      trace_id  = trace_id,
  )

  # ANTA post_check 起動判定
  if is_ok(response) and snap_id:
      asyncio.create_task(...)
"""

from typing import Any, Dict, Optional


# ── ステータス値の定数（タイポ防止） ─────────────────────────────────────────
STATUS_SUCCESS   = "success"
STATUS_DRY_RUN   = "dry_run"
STATUS_FAILURE   = "failure"
STATUS_ERROR     = "error"
STATUS_BLOCKED   = "blocked"
STATUS_SKIPPED   = "skipped"
STATUS_NO_CHANGE = "no_change"

# deploy 成功とみなすステータス群（ANTA post_check 起動判定等で使用）
OK_STATUSES = frozenset({
    STATUS_SUCCESS,
    STATUS_DRY_RUN,
    STATUS_NO_CHANGE,
    "all_success",    # NETCONF マルチタスク完走時（既存Hub互換）
})


# ── 基底スキーマ組み立て関数（新規Hub向け） ──────────────────────────────────

def make_response(
    status:    str,
    route:     str,
    routed_to: str,
    summary:   str,
    result:    Dict[str, Any],   # ← "result" キーを維持（既存app_a2a.py互換）
    trace_id:  str = "",
    *,
    # 将来の人間承認ゲート向け予約フィールド
    uncertainty:                 Optional[str] = None,
    human_confirmation_required: bool          = False,
    recommended_action:          Optional[str] = None,
    # 追加フィールド（ルートごとに必要なもの）
    **extra: Any,
) -> Dict[str, Any]:
    """
    Hub レスポンスの基底 dict を返す（新規Hub向け）。

    "result" キーを既存と同名にしているため、
    このモジュールで作ったレスポンスは app_a2a.py が
    inner = result.get("result", {}) で読める。

    Args:
        status:    STATUS_* 定数のいずれか
        route:     "write" | "read" | "diagnose" など
        routed_to: 転送先 URL
        summary:   UI向け一言サマリ（日本語）
        result:    A2Aサーバが返した dict（= inner）
        trace_id:  トレースID
        **extra:   ルート固有の追加フィールド
                   例: xml="...", task_summaries=[...], eapi_cmds=[...]
    """
    resp: Dict[str, Any] = {
        "status":    status,
        "route":     route,
        "routed_to": routed_to,
        "summary":   summary,
        "result":    result,
        **extra,
    }

    if trace_id:
        resp["trace_id"] = trace_id

    # 将来フィールド: 値が入っているときだけ追加
    if uncertainty is not None:
        resp["uncertainty"] = uncertainty
    if human_confirmation_required:
        resp["human_confirmation_required"] = True
    if recommended_action is not None:
        resp["recommended_action"] = recommended_action

    return resp


def make_error_response(
    route:     str,
    routed_to: str,
    message:   str,
    trace_id:  str = "",
) -> Dict[str, Any]:
    """エラー時の簡易組み立て。status="error" 固定。"""
    return make_response(
        status    = STATUS_ERROR,
        route     = route,
        routed_to = routed_to,
        summary   = message,
        result    = {"message": message},
        trace_id  = trace_id,
        message   = message,    # result直下にも置く（既存app_a2a.py互換）
    )


def is_ok(response: Dict[str, Any]) -> bool:
    """
    レスポンスの status が成功系かどうかを返す。
    ANTA post_check 起動判定などに使う。

    既存Hubのレスポンスにも使える。

    使用例（task_decompose_a2a_server.py の修正版）:
        from core.response_schema import is_ok
        if is_ok(response) and snap_id:
            response["anta_post_check"] = "running"
            _trace_store[trace_id]["deploy_result"] = response
            asyncio.create_task(_auto_post_check(...))
    """
    return response.get("status", "") in OK_STATUSES
