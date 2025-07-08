import os
import pandas as pd

# 📁 루트 디렉토리 설정
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INPUT_PATH = os.path.join(BASE_DIR, "data", "raw", "convenience__raw.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "raw", "convenience_usual__raw.csv")

# ❌ 제외할 업태 목록
exclude_types = ["전통찻집", "키즈카페", "철도역구내", "관광호텔", "유원지", "떡카페", "푸드트럭", "다방"]

# ✅ 파일을 open()으로 열고, 깨진 문자는 무시해서 pandas로 읽기
with open(INPUT_PATH, 'r', encoding='cp949', errors='replace') as f:
    df = pd.read_csv(f, dtype=str)

# ✅ 컬럼 체크
if "영업상태명" not in df.columns or "업태구분명" not in df.columns:
    print(f"[ERROR] 필수 컬럼이 존재하지 않습니다. 컬럼 목록: {df.columns.tolist()}")
    exit()

# ✅ 조건 필터링
filtered_df = df[
    (df["영업상태명"] == "영업/정상") &
    (~df["업태구분명"].isin(exclude_types))
].copy()

# 💾 저장
filtered_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
print(f"[완료] 저장됨: {OUTPUT_PATH} (총 {len(filtered_df)}건)")
