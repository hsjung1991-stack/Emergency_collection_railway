import os
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime


def fetch_er_data(service_key: str) -> list[dict]:
    """공공데이터포털 응급실 실시간 가용 병상 정보 수집"""
    url = "http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEmrrmRltmUsefulSckbdInfoInqire"
    params = {
        "serviceKey": service_key,
        "STAGE1": "서울특별시",
        "pageNo": "1",
        "numOfRows": "100",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "xml")
    items = soup.find_all("item")

    if not items:
        print("❌ 수집된 데이터가 없습니다. API 키나 URL을 확인하세요.")
        return []

    def text(item, tag: str) -> str:
        node = item.find(tag)
        return node.text.strip() if node else ""

    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return [
        {
            "기관코드": text(item, "hpid"),
            "기관명": text(item, "dutyName"),
            "일반응급실병상_hvec": text(item, "hvec"),
            "수술실_hvoc": text(item, "hvoc"),
            "CT가용": text(item, "hvctayn"),
            "MRI가용": text(item, "hvmriayn"),
            "신경중환자실": text(item, "hvcc"),
            "신경외과중환자실": text(item, "hv6"),
            "조영촬영기": text(item, "hvangioayn"),
            "흉부중환자실": text(item, "hvccc"),
            "인공호흡기": text(item, "hvventiayn"),
            "외상중환자실": text(item, "hv9"),
            "업데이트시각": text(item, "hvidate"),
            "수집시각": collected_at,
        }
        for item in items
    ]


def save_to_supabase(data: list[dict], db_url: str) -> None:
    """Supabase Pooler(PostgreSQL)에 데이터 저장"""
    if not data:
        return

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                # 테이블이 없으면 생성
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS er_realtime_log (
                        id          BIGSERIAL PRIMARY KEY,
                        기관코드          TEXT,
                        기관명            TEXT,
                        일반응급실병상_hvec  TEXT,
                        수술실_hvoc        TEXT,
                        "CT가용"           TEXT,
                        "MRI가용"          TEXT,
                        신경중환자실         TEXT,
                        신경외과중환자실      TEXT,
                        조영촬영기          TEXT,
                        흉부중환자실         TEXT,
                        인공호흡기          TEXT,
                        외상중환자실         TEXT,
                        업데이트시각         TEXT,
                        수집시각            TEXT
                    )
                """)

                columns = data[0].keys()
                col_str = ", ".join(f'"{c}"' for c in columns)
                rows = [tuple(row[c] for c in columns) for row in data]

                execute_values(
                    cur,
                    f"INSERT INTO er_realtime_log ({col_str}) VALUES %s",
                    rows,
                )
        print(f"✅ Supabase 저장 성공! 총 {len(data)}건 적재 완료.")
    finally:
        conn.close()


def run() -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 시작...")

    service_key = os.environ.get("PUBLIC_DATA_SERVICE_KEY")
    db_url = os.environ.get("DATABASE_URL")

    if not service_key:
        raise EnvironmentError("PUBLIC_DATA_SERVICE_KEY 환경 변수가 없습니다.")
    if not db_url:
        raise EnvironmentError("DATABASE_URL 환경 변수가 없습니다.")

    data = fetch_er_data(service_key)
    save_to_supabase(data, db_url)


if __name__ == "__main__":
    run()
