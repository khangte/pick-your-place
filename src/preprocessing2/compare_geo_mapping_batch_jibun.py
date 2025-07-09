"""
    지번주소의 경우
    행정동 법정동을 직접 매핑하는 것이 더 나은 방법이라 판단.
"""
import os
import sys
import re
import time
import pandas as pd
from tqdm import tqdm

# 🔐 경로 설정
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
os.chdir(project_root)
sys.path.append(os.path.join(project_root, "src"))

from src.geocoding.vworld_geocode import jibun_address_to_coordinates
from mapping_utils import perform_geojson_mapping

# ✅ 파일 및 설정
INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/processed_counts2"
GEOJSON_PATH = "data/reference/Seoul_HangJeongDong.geojson"
FULL_GEOJSON_PATH = "data/reference/HangJeongDong_ver20250401.geojson"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ✅ 처리 대상
TARGET_FILES = [
    "store_usual__raw.csv",
]

ADDRESS_COLUMNS = {
    "store_usual__raw.csv": "지번주소",
}

summary = []

for filename in TARGET_FILES:
    print(f"\n📂 [처리 시작] {filename}")
    address_col = ADDRESS_COLUMNS.get(filename)
    if not address_col:
        print(f"[❌ 주소 칼럼명 누락] {filename}")
        continue

    input_path = os.path.join(INPUT_DIR, filename)

    try:
        df = pd.read_csv(input_path, dtype=str)
    except UnicodeDecodeError:
        print(f"[⚠️ 인코딩 재시도] {filename} → cp949")
        df = pd.read_csv(input_path, dtype=str, encoding="cp949")

    # ✅ 주소 → 위경도
    coords = []
    start0 = time.time()
    for addr in tqdm(df[address_col].fillna(""), desc="📌 주소 → 좌표"):

        if not addr or len(addr) < 5:
            coords.append((None, None))
            continue

        coord = jibun_address_to_coordinates(addr)
        coords.append(coord if coord else (None, None))

    df[["longitude", "latitude"]] = pd.DataFrame(coords, index=df.index)
    df_valid = df.dropna(subset=["longitude", "latitude"]).copy()
    df_valid["longitude"] = df_valid["longitude"].astype(float)
    df_valid["latitude"] = df_valid["latitude"].astype(float)
    time0 = time.time() - start0

    # ✅ GeoJSON 공간조인
    start2 = time.time()
    geojson_result = perform_geojson_mapping(
        input_path=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        geojson_path=GEOJSON_PATH,
        full_geojson_path=FULL_GEOJSON_PATH,
        filename=filename,
    )
    time2 = time.time() - start2

    # ✅ 좌표 변환 결과 저장
    df_valid.to_csv(
        os.path.join(OUTPUT_DIR, filename.replace("__raw", "__with_coords")),
        index=False, encoding="utf-8-sig"
    )

    # ✅ GeoJSON 매핑된 행 단위 데이터 저장
    mapped_df = geojson_result.get("mapped_df")
    if mapped_df is not None:
        mapped_df.to_csv(
            os.path.join(OUTPUT_DIR, filename.replace("__raw", "__mapped")),
            index=False, encoding="utf-8-sig"
        )

    # ✅ 요약 결과
    summary.append({
        "filename": filename,
        "total_rows": len(df),
        "coord_success": len(df_valid),
        "api_code_success": 0,  # 방법1 비활성화
        "geojson_success": geojson_result.get("mapped_rows", 0),
        "coord_time_sec": round(time0, 2),
        "api_time_sec": 0,
        "geojson_time_sec": round(time2, 2),
    })

    time.sleep(0.2)

# ✅ 요약 저장
summary_df = pd.DataFrame(summary)
summary_df.to_csv(os.path.join(OUTPUT_DIR, "___summary.csv"), index=False, encoding="utf-8-sig")
print("\n✅ 전체 처리 완료! 요약 저장 → ___summary.csv")
