import os
import sys
import pandas as pd
from tqdm import tqdm

# ✅ src 경로 등록
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# ✅ 내부 함수 import
from src.geocoding.kakao_api_function import reverse_geocode
from src.geocoding.admin_mapper import (
    smart_parse_gu_and_dong,
    extract_gu_and_dong,
    get_gu_dong_codes,
)
from src.geocoding.vworld_geocode import coordinates_to_jibun_address

# ✅ 경로 설정
INPUT_PATH = "data/raw/park__raw.csv"
OUTPUT_PATH = "data/processed/park__processed.csv"
FAIL_PATH = "data/processed/park__failed.csv"

# ✅ 사용할 컬럼
usecols = ["P_IDX", "P_PARK", "P_ZONE", "P_ADDR", "LATITUDE", "LONGITUDE"]
df = pd.read_csv(INPUT_PATH, usecols=usecols).dropna(subset=["P_ADDR"]).copy()

# ✅ 서울특별시만 필터링
df = df[df["P_ADDR"].str.contains("서울특별시", na=False)].copy()

# ✅ 결과 컬럼 추가
df["gu_name"] = None
df["dong_name"] = None
df["gu_code"] = None
df["dong_code"] = None

# ✅ 실패 저장용 리스트
failures = []

# ✅ 메인 처리 루프
for idx, row in tqdm(df.iterrows(), total=len(df)):
    addr = row["P_ADDR"]
    lat = row["LATITUDE"]
    lon = row["LONGITUDE"]
    p_idx = row["P_IDX"]

    gu, dong = smart_parse_gu_and_dong(addr)

    # ⛔ 동 추출 실패 시 좌표 기반 추출
    if not gu or not dong:
        try:
            jibun_addr = coordinates_to_jibun_address(lon, lat)
            gu, dong = extract_gu_and_dong(jibun_addr)
        except Exception as e:
            print(f"[좌표 fallback 실패] P_IDX={p_idx}, lat={lat}, lon={lon} → {e}")
            failures.append({
                "P_IDX": p_idx,
                "P_ADDR": addr,
                "LATITUDE": lat,
                "LONGITUDE": lon,
                "reason": "구/동 추출 실패",
                "gu": None,
                "dong": None
            })
            continue

    # ✅ 행정동 코드 매핑
    if gu and dong:
        gu_code, dong_code = get_gu_dong_codes(gu, dong)
        if gu_code and dong_code:
            df.at[idx, "gu_name"] = gu
            df.at[idx, "dong_name"] = dong
            df.at[idx, "gu_code"] = gu_code
            df.at[idx, "dong_code"] = dong_code
        else:
            print(f"[행정동 코드 매핑 실패] {gu} {dong}")
            failures.append({
                "P_IDX": p_idx,
                "P_ADDR": addr,
                "LATITUDE": lat,
                "LONGITUDE": lon,
                "reason": "행정동 코드 매핑 실패",
                "gu": gu,
                "dong": dong
            })
    else:
        print(f"[동명 최종 실패] {addr}")
        failures.append({
            "P_IDX": p_idx,
            "P_ADDR": addr,
            "LATITUDE": lat,
            "LONGITUDE": lon,
            "reason": "구/동 추출 실패",
            "gu": gu,
            "dong": dong
        })

# ✅ 수동 매핑 (P_IDX 기반)
manual_dong_map = {
    6:  {"gu_name": "종로구", "dong_name": "청운효자동"},
    9:  {"gu_name": "성동구", "dong_name": "금호1가동"},
    24: {"gu_name": "관악구", "dong_name": "청룡동"},
    72: {"gu_name": "노원구", "dong_name": "공릉2동"},
    107:{"gu_name": "종로구", "dong_name": "사직동"},
    108:{"gu_name": "관악구", "dong_name": "은천동"},
    111:{"gu_name": "은평구", "dong_name": "진관동"},
}

for p_idx, info in manual_dong_map.items():
    gu = info["gu_name"]
    dong = info["dong_name"]
    gu_code, dong_code = get_gu_dong_codes(gu, dong)

    df.loc[df["P_IDX"] == p_idx, "gu_name"] = gu
    df.loc[df["P_IDX"] == p_idx, "dong_name"] = dong
    df.loc[df["P_IDX"] == p_idx, "gu_code"] = gu_code
    df.loc[df["P_IDX"] == p_idx, "dong_code"] = dong_code

# ✅ 실패 데이터프레임 저장 (수동 매핑 반영된 항목 제외)
fail_df = pd.DataFrame(failures)
fail_df = fail_df[~fail_df["P_IDX"].isin(manual_dong_map.keys())]
fail_df.to_csv(FAIL_PATH, index=False, encoding="utf-8-sig")

# ✅ 최종 저장
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
print(f"✅ 공원 전처리 최종 완료: {OUTPUT_PATH}")
print(f"⚠️ 실패 데이터 저장 완료: {FAIL_PATH}")
