# src/preprocessing2/geojson_batch_runner.py
import os
import sys
import time
import pandas as pd
from src.preprocessing2.mapping_utils import perform_geojson_mapping

# 🔐 경로 설정
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
os.chdir(project_root)
sys.path.append(os.path.join(project_root, "src"))

INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/processed_counts2"
GEOJSON_PATH = "data/reference/Seoul_HangJeongDong.geojson"
FULL_GEOJSON_PATH = "data/reference/HangJeongDong_ver20250401.geojson"

TARGET_FILES = [
    "bus_stop__raw.csv",
    "cctv__raw.csv",
    "hospital__raw.csv",
    "library__raw.csv",
    "park__raw.csv",
    "safety_bell__raw.xlsx",
    "street_light__raw.csv",
    "subway_station__raw.csv",
]

summary = []
start = time.time()

for fname in TARGET_FILES:
    print(f"\n[▶] {fname} 처리 중...")
    result = perform_geojson_mapping(
        input_path=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        geojson_path=GEOJSON_PATH,
        full_geojson_path=FULL_GEOJSON_PATH,
        filename=fname,
    )
    summary.append(result)

summary_df = pd.DataFrame(summary)
summary_df.to_csv(os.path.join(OUTPUT_DIR, "__summary.csv"), index=False, encoding="utf-8-sig")

print(f"\n✅ 전체 완료! 총 소요 시간: {time.time() - start:.2f}초")
