"""
Railway 등 클라우드 환경에서 5분마다 collector.run()을 실행하는 스케줄러.
컴퓨터가 꺼져 있어도 서버에서 계속 동작합니다.
"""

import time
import traceback
from datetime import datetime

from collector import run

INTERVAL_SECONDS = 5 * 60  # 5분


def main() -> None:
    print(f"[스케줄러 시작] {INTERVAL_SECONDS}초 간격으로 수집합니다.")

    while True:
        try:
            run()
        except Exception:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ 수집 중 오류 발생:")
            traceback.print_exc()

        print(f"  ⏳ {INTERVAL_SECONDS}초 후 다시 수집합니다...\n")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
