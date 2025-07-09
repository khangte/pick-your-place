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

from src.geocoding.vworld_geocode import road_address_to_coordinates
from src.geocoding.kakao_api_function import address_to_coord, coord_to_region
from src.geocoding.admin_mapper import get_gu_dong_codes
from mapping_utils import perform_geojson_mapping

# ✅ 파일 및 설정
INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/processed_counts2"
GEOJSON_PATH = "data/reference/Seoul_HangJeongDong.geojson"
FULL_GEOJSON_PATH = "data/reference/HangJeongDong_ver20250401.geojson"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ✅ 처리 대상
TARGET_FILES = [
    "police_office__raw.csv",
    "school__raw.csv",
]

ADDRESS_COLUMNS = {
    "police_office__raw.csv": "경찰서주소",
    "school__raw.csv": "도로명주소",
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
        addr = re.sub(r"\(.*?\)", "", addr)
        if "," in addr:
            addr = addr.split(",")[0]
        addr = addr.replace("번지", "").replace("일원", "")
        addr = re.sub(r"(\d+)의\s*(\d+)", r"\1 \2", addr)
        addr = re.sub(r"\b(지하|지상|\d+층|\d+호|\d+동)\b", "", addr)
        addr = re.sub(r"\s+", " ", addr).strip()

        if not addr or len(addr) < 5:
            coords.append((None, None))
            continue

        coord = road_address_to_coordinates(addr)
        # coord = address_to_coord(addr)
        coords.append(coord if coord else (None, None))

    df[["longitude", "latitude"]] = pd.DataFrame(coords, index=df.index)
    df_valid = df.dropna(subset=["longitude", "latitude"]).copy()
    df_valid["longitude"] = df_valid["longitude"].astype(float)
    df_valid["latitude"] = df_valid["latitude"].astype(float)
    time0 = time.time() - start0

    # # ✅ 방법 1: Kakao API + 동코드 매핑
    # start1 = time.time()
    # gu_list, dong_list, gu_code_list, dong_code_list = [], [], [], []
    #
    # for lon, lat in tqdm(zip(df_valid["longitude"], df_valid["latitude"]), total=len(df_valid), desc="📍 방법1: API"):
    #     region = coord_to_region(lon, lat)
    #     if region:
    #         _, gu, dong = region
    #         gu_list.append(gu)
    #         dong_list.append(dong)
    #         try:
    #             gu_code, dong_code = get_gu_dong_codes(gu, dong)
    #             gu_code_list.append(gu_code)
    #             dong_code_list.append(dong_code)
    #         except:
    #             gu_code_list.append(None)
    #             dong_code_list.append(None)
    #     else:
    #         gu_list.append(None)
    #         dong_list.append(None)
    #         gu_code_list.append(None)
    #         dong_code_list.append(None)
    #
    # df_valid["gu_name"] = gu_list
    # df_valid["dong_name"] = dong_list
    # df_valid["gu_code"] = gu_code_list
    # df_valid["dong_code"] = dong_code_list
    # time1 = time.time() - start1

    # ✅ 방법 2: GeoJSON 공간조인
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
