# a2a-interconnect

AWS/Googleが共同で公開しているオープンな対称型OpenAPI 3.0仕様「**Connection Coordinator API**」（クラウドプロバイダ間のL3相互接続を調整する仕様、[aws/Interconnect](https://github.com/aws/Interconnect)）を、A2A（Agent2Agent）プロトコルで実装したものです。

**本プロジェクトの第一目的は、この仕様への準拠そのものです。** 後述する決定論的なベンダー実行ツールは、その目的を達成するための**手段**であり、目的そのものではありません。これは、ネゴシエーションの結果を、自然言語ベースの設定生成が持ち込みがちな非決定性を排除した形で、実際のネットワーク機器に確実に反映させるために存在します。

## 概要

2つの組織（*Negotiator*と*Responder*）が、それぞれ自分側を代表する形でこのエージェントを1つずつ起動します。両エージェントは、少数のA2Aメッセージをやり取りして以下を行います。

1. 接続要求を発行し、`ActivationKey`を発行する
2. 顧客（またはその代理を務める自動化エージェント）が、そのキーを相手側へ持ち込む
3. キーの真正性を検証する（顧客経由ではなく、当事者同士で直接行う）
4. Responderが提示する範囲の中から、VLAN・ASN・サブネット・MTUの具体値をNegotiatorが選ぶ
5. 合意した設定を、それぞれ自分側のネットワーク機器へデプロイする
6. 接続が確立したことを相互に確認する

自然言語入力が関与するのは、**「接続を作成して」という最初の要求のみ**です。それ以降は完全に構造化されたデータのみで進み、パラメータ交渉・VLAN/ASN/サブネットの選定・機器設定の生成のいずれにもLLMは一切関与しません。

## アーキテクチャ

本リポジトリは**Integration Layer**に位置します。Interconnectプロトロルの交渉と、「何を設定すべきか」の決定を担いますが、ネットワーク機器へは直接接続しません。実際の機器設定は**Vendor Core**（NETCONFセッション・ベンダー固有のXML生成・当該NOS向けの独自のポリシー/安全チェックを持つ、別サービス）に委譲します。

```
Negotiator（本リポジトリ）  <──A2A/HTTP──>  Responder（本リポジトリ）
        │                                        │
        ▼ /deploy_structured                     ▼ /deploy_structured
  Vendor Core（Junos、Aristaなど）          Vendor Core（Junos、Aristaなど）
        │                                        │
        ▼ NETCONF                                ▼ NETCONF
  ネットワーク機器                          ネットワーク機器
```

Vendor Coreは**本リポジトリには含まれません**。

### `/deploy_structured`契約

Vendor Coreは、デプロイDAG（[設計上の要点](#設計上の要点)参照）のタスク1つにつき1回呼び出される、単一の決定論的エンドポイントを公開します。

```
POST /deploy_structured
request:
  {
    "task_type": str,   # Vendor Core側でどのXMLテンプレート・governance
                         # 分類を適用するかを選ぶキー
    "params":    dict,  # task_typeごとの構造化パラメータ
    "device": {"ip": str, "port": str, "username": str, "password": str}
  }
response:
  { "status": "success" | "dry_run" | "no_change" | "all_success" | "failure" | ..., ... }
```

この呼び出しには、どちら側にも自然言語もLLMも一切関与しません。本エージェントが送信する時点で、`task_type`・`params`は既に完全に解決済みの構造化データです。Vendor Core側は、これをベンダー固有の設定（例：NETCONF XML）に変換し、自身のポリシーチェックを適用し、単一のコミットとしてデプロイした上で、`status`フィールドから成否を判定できる結果を返すことが期待されます。新しいNOSへの対応は、このエンドポイント1つを実装するだけで済み、本リポジトリ側の変更は不要です。

## 同梱ファイル

| ファイル | 役割 |
|---|---|
| `interconnect_bridge_a2a_server.py` | エージェント本体：A2Aスキル、ネゴシエーション状態機械、決定論的タスクDAG生成、デプロイオーケストレーション |
| `governance_client.py` | 外部のポリシー/ガバナンスサービス（任意）への薄いHTTPクライアント。到達不能時はfail-safeに倒れる（書き込み系は拒否、読み取り系は許可）ため、本エージェントはこれ無しでも単体で動作する |
| `llm_factory.py` | 共通LLM初期化・フォールバックヘルパー（Groqをプライマリ、Vertex AI経由のGeminiをフォールバックとして使用。自然言語による意図抽出スキルでのみ使用） |
| `response_schema.py` | 成功/エラーレスポンスの形を揃えるための小さなヘルパー |

## 必要環境

```bash
pip install a2a-sdk fastapi uvicorn httpx pydantic langchain-openai
# 任意（llm_factory.pyのGeminiフォールバックを使う場合のみ）:
pip install "langchain-google-vertexai>=3.2.4"
```

GeminiへのアクセスはVertex AI経由であり、単体のGemini APIキー方式ではありません。GCPプロジェクトとApplication Default Credentials（`gcloud auth application-default login`）、またはサービスアカウント（`roles/aiplatform.user`）が必要です。`langchain-google-vertexai`が未インストール、またはGCP認証が未設定の場合も、Groq単体でそのまま動作します。

外部のガバナンス/ポリシーサービスは任意です（上記`governance_client.py`参照）。

## 設定

すべて環境変数で設定します。

| 変数 | デフォルト | 意味 |
|---|---|---|
| `A2A_PORT` | `8202` | 本エージェントの待受ポート |
| `PROVIDER_ID` | `aws` | 自分自身のidentity（例：`aws`、`gcp`） |
| `VENDOR_ID` | `junos` | 自分側がどのVendor Core経由でデプロイするか（`junos` \| `ceos`） |
| `DEVICE_IP` / `DEVICE_USERNAME` / `DEVICE_PASSWORD` | `172.20.100.31` / `admin` / `admin` | 自分側の機器の認証情報（Vendor Coreへ渡す） |
| `JUNOS_IFACE` | `et-0/0/2` | 物理インターフェース名（Junos Vendor Core向け） |
| `CEOS_IFACE` | `Ethernet1` | 物理インターフェース名（Arista Vendor Core向け） |
| `PEER_BASE_URL_<相手のPROVIDER名>` | なし | 相手側エージェントの接続先URL。**「自分」ではなく「相手」の識別子**をキーにする点に注意（例：`aws`役では`PEER_BASE_URL_GCP=http://localhost:8290`） |
| `PROVIDER_ASN_<PROVIDER名>` | なし | 各プロバイダの実在BGP ASN。そのプロバイダの識別子をキーにする。ネゴシエーションに関わる全プロバイダ分を、両側に設定する必要がある（例：`PROVIDER_ASN_AWS=65001 PROVIDER_ASN_GCP=65002`） |
| `CEOS_HUB_URL` / `JUNOS_HUB_URL` | `http://localhost:8000` / `http://localhost:8020` | Vendor Core HubのURL |
| `GOVERNANCE_A2A_URL` | `http://localhost:8190` | 外部ガバナンスサービス（任意） |
| `GROQ_API_KEY` | なし | 自然言語による意図抽出スキルに必要 |

## スキル一覧

| スキル | 役割 |
|---|---|
| `create_connection` | Negotiator側：構造化パラメータから接続要求を作成し`ActivationKey`を発行 |
| `create_connection_nl` | 同上、ただしパラメータを自然言語要求から抽出 |
| `accept_connection_nl` | Responder側：顧客が持ち込んだ`ActivationKey`を受理（キー・ID等の重要な値は構造化パラメータで受け取り、自由文からは値を抽出しない） |
| `verify_activation_key` | 当事者間でのキー検証（Responder→Negotiator） |
| `generate_feature_guidance` | 当事者間での範囲交渉（Negotiator→Responder） |
| `create_feature` | 具体的な設定の提案・受理、デプロイの起動 |
| `approve` | ガバナンス層がレビュー対象と判定したデプロイに対する、人間承認ゲート |
| `notify_connection_status` | 当事者間での完了通知 |
| `get_negotiation_status` | 接続の現在の状態を照会 |

## 設計上の要点

- **実行は決定論的、交渉にLLMを使わない**：LLMが関与するのは、`create_connection_nl`において自由文を`{environment, bandwidth_mbps, peer_provider}`に変換するステップのみ。VLAN/ASN/サブネットの選定、XML生成、機器デプロイはすべて、モデルを一切介さない通常のコードで行う。これは、機器設定をLLMで生成した際に観測された非決定性への、意図的な対応。
- **Responderが範囲を提示し、Negotiatorがその中から選ぶ**：どちらか一方が最終値を一方的に押し付けるのではなく、仕様が意図する役割分担に合わせている。
- **Capability/DAG層**（`CAPABILITY_REGISTRY` / `CEOS_CAPABILITY_REGISTRY`）が、各ベンダーのデプロイに必要な要素とその依存関係を宣言し、実際の実行順序はその宣言から導出される（ハードコードしない）。新しいベンダーを追加する作業は、新しいオーケストレーションロジックではなく、新しいレジストリを1つ書くだけで済む。
- **実在のプロバイダ固有ASN**を使う（接続ごとに動的採番される値ではない）。ASNは実在するネットワーク事業者の固定識別子であり、接続ごとに交渉すべき値ではないため。

## ライセンス

MIT — `LICENSE`参照。
