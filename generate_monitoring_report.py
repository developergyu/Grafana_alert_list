import json
import re
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== [환경 설정 변수] ====================
GRAFANA_URL = "https://grafana.com.ezpc.internal"
API_TOKEN = "API_TOKEN"
# ==========================================================

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
}


def fetch_smart_targets_dynamic(prom_expr, datasource_uid):
    """알람별 라벨 구조를 분석하여,
    진짜 인스턴스(서버)명 또는 애플리케이션 Pod명을 판별합니다.
    """
    target_list = []
    if not prom_expr or not datasource_uid:
        return target_list

    query_url = f"{GRAFANA_URL}/api/datasources/uid/{datasource_uid}/resources/api/v1/query"

    try:
        params = {"query": prom_expr}
        res = requests.get(
            query_url, headers=headers, params=params, verify=False
        )

        if res.status_code == 404:
            alt_url = f"{GRAFANA_URL}/api/datasources/proxy/uid/{datasource_uid}/api/v1/query"
            res = requests.get(
                alt_url, headers=headers, params=params, verify=False
            )

        if res.status_code == 200:
            result_data = res.json().get("data", {}).get("result", [])

            for item in result_data:
                metric = item.get("metric", {})

                instance_val = metric.get("instance")
                pod_val = metric.get(
                    "pod", metric.get("kubernetes_pod", metric.get("pod_name"))
                )

                dynamic_target = None

                # 🎯 [핵심 방어선] 에이전트성 시스템 Pod 이름인지 검사 패턴
                # node-exporter, prom-node, kube-state-metrics 등을 걸러냅니다.
                is_infra_agent_pod = False
                if pod_val:
                    is_infra_agent_pod = any(
                        agent in pod_val.lower()
                        for agent in [
                            "node-exporter",
                            "prometheus-node",
                            "kube-state",
                            "metrics-server",
                            "fluentd",
                            "prom-node",
                        ]
                    )

                # Case 1: Pod 라벨이 존재하지만, 인프라 수집기 에이전트 Pod인 경우 ➡️ 무조건 서버명(instance)이 우선
                if pod_val and is_infra_agent_pod:
                    dynamic_target = instance_val if instance_val else pod_val

                # Case 2: 순수 애플리케이션 Pod 알람인 경우 (에이전트가 아님) ➡️ Pod명을 우선 채택
                elif pod_val:
                    dynamic_target = pod_val

                # Case 3: Pod 라벨이 아예 없는 순수 인프라/OS 알람인 경우 ➡️ 서버명(instance) 채택
                else:
                    dynamic_target = (
                        instance_val
                        if instance_val
                        else metric.get("node", "unknown-target")
                    )

                if dynamic_target and dynamic_target not in target_list:
                    target_list.append(dynamic_target)

    except Exception as e:
        print(
            f"   ⚠️ 데이터소스 [{datasource_uid}] 쿼리 가동 중 에러 발생: {e}"
        )

    return target_list


def parse_all_datasources_alerts():
    """Grafana의 모든 알람을 돌며, 각각에 설정된 데이터소스를 추적하여 타겟을 수집합니다."""
    print("[1/2] Grafana에서 전체 알람 마스터 데이터 추출 중...")
    rules_url = f"{GRAFANA_URL}/api/v1/provisioning/alert-rules"
    res = requests.get(rules_url, headers=headers, verify=False)

    if res.status_code != 200:
        print(f"❌ Grafana API 호출 실패 (Status: {res.status_code})")
        return []

    alert_rules = res.json()
    parsed_rows = []

    print("\n[2/2] ▶ 알람별 고유 데이터소스 매핑 및 실시간 타겟 추출 시작...")

    for rule in alert_rules:
        if rule.get("isPaused", False):
            continue

        title = rule.get("title", "").strip()
        group = rule.get("ruleGroup", "").upper()
        duration = rule.get("for", "0s")

        threshold = "조건식 참조"
        prom_expr = ""
        alert_datasource_uid = None

        for d in rule.get("data", []):
            model = d.get("model", {})
            if "expr" in model:
                prom_expr = model.get("expr", "")
                datasource_obj = d.get("datasourceUid") or model.get(
                    "datasource", {}
                ).get("uid")
                if datasource_obj and datasource_obj != "__expr__":
                    alert_datasource_uid = datasource_obj

            if model.get("type") == "math":
                threshold = (
                    model.get("expression", "")
                    .replace("$C", "평균")
                    .replace("$D", "최대")
                    .replace("||", "또는")
                )

        if alert_datasource_uid:
            print(
                f" ➡️ 알람명: [{title}] | 사용 데이터소스 UID: [{alert_datasource_uid}]"
            )
            real_targets = fetch_smart_targets_dynamic(
                prom_expr, alert_datasource_uid
            )
        else:
            print(
                f" ➡️ 알람명: [{title}] | ⚠️ 유효한 프로메테우스 데이터소스 UID를 찾지 못해 스킵합니다."
            )
            real_targets = []

        print(f"    └ 실시간 탐지된 대상: {len(real_targets)}개")

        for target in real_targets:
            receiver = rule.get("notification_settings", {}).get(
                "receiver", "기본수신처"
            )
            row = {
                "모니터링 대상 (Instance/Pod)": target,
                "분류항목": group,
                "알람 이름": title,
                "출처 데이터소스 UID": alert_datasource_uid,
                "탐지 지연 시간(for)": duration,
                "알람 발생 기준값": threshold,
                "실행된 PromQL 쿼리": prom_expr,
                "수신 그룹(Receiver)": receiver,
            }
            parsed_rows.append(row)

    return parsed_rows


def save_to_excel(
    parsed_rows, filename="grafana_all_datasources_monitoring_report.xlsx"
):
    if not parsed_rows:
        print("❌ 매핑된 데이터가 존재하지 않아 엑셀 생성을 취소합니다.")
        return

    df = pd.DataFrame(parsed_rows)
    df = df.sort_values(by=["알람 이름", "모니터링 대상 (Instance/Pod)"])

    df.to_excel(filename, index=False)
    print(
        f"\n🎉 전체 데이터소스 기준 매핑 완료! 엑셀 생성됨: {filename} (총 {len(df)}행)"
    )


if __name__ == "__main__":
    all_mapped_rows = parse_all_datasources_alerts()
    save_to_excel(all_mapped_rows)
