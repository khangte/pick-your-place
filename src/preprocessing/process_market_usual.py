import os
import pandas as pd

# 📁 루트 디렉토리 설정
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INPUT_PATH = os.path.join(BASE_DIR, "data", "raw", "market__raw.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "raw", "market_usual__raw.csv")

# ✅ 파일을 open()으로 열고, 깨진 문자는 무시해서 pandas로 읽기
with open(INPUT_PATH, 'r', encoding='cp949', errors='replace') as f:
    df = pd.read_csv(f, dtype=str)


# ✅ 조건 필터링
filtered_df = df[df["영업상태명"] == "영업/정상"].copy()

# 💾 저장
filtered_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
print(f"[완료] 저장됨: {OUTPUT_PATH} (총 {len(filtered_df)}건)")
