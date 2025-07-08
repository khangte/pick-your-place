import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.geocoding.vworld_geocode import road_address_to_coordinates
from src.geocoding.admin_mapper import extract_gu_and_dong, get_gu_dong_codes

def load_market_csv(path: str = "../../data/raw/market__raw.csv") -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"파일이 존재하지 않습니다: {path}")

    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="cp949")

    print(f"[로드 완료] {path} - {len(df)}건")
    return df

def process_market_data(df: pd.DataFrame) -> pd.DataFrame:
    needed_cols = ['도로명주소', '시군구', '시도', '시장명', '시장코드', '지번주소']
    df = df[[col for col in needed_cols if col in df.columns]]
    # df = df[df['시도'] == '서울특별시']

    df = df.rename(columns={
        '도로명주소': 'road_address',
        '시군구': 'gu_name',
        '시장명': 'market_name',
        '시장코드': 'market_code',
        '지번주소': 'jibun_address'
    })

    print(f"[전처리 완료] {len(df)}건")
    return df

def mapping_process(df):
    df = process_market_data(df)

    # 도로명 주소 → (경도, 위도)
    coords = df['road_address'].apply(lambda addr: road_address_to_coordinates(addr))
    df['lon'], df['lat'] = zip(*coords)

    # 지번주소 → 자치구명, 법정동명
    df[['gu_tmp', 'legal_dong']] = df['jibun_address'].apply(
        lambda addr: pd.Series(extract_gu_and_dong(addr))
    )

    # fallback: 지번주소 실패 시 도로명 주소로 추출
    need_fallback = df['legal_dong'].isnull() | (df['legal_dong'] == '')
    fallback_addrs = df.loc[need_fallback, 'road_address'].apply(lambda addr: pd.Series(extract_gu_and_dong(addr)))
    df.loc[need_fallback, 'gu_tmp'] = fallback_addrs[0]
    df.loc[need_fallback, 'legal_dong'] = fallback_addrs[1]

    # 법정동명 → 행정동 코드 및 행정동명
    df[['gu_code', 'dong_code', 'dong_name']] = df.apply(
        lambda r: pd.Series(get_gu_dong_codes(r['gu_tmp'], r['legal_dong'])), axis=1
    )

    # 자치구명 최종 적용
    df['gu_name'] = df['gu_tmp']

    # 결과 필터링
    df_result = df[~df['dong_code'].isnull()].copy()
    df_result = df_result[[
        'gu_code', 'dong_code',
        'gu_name', 'dong_name',
        'jibun_address', 'road_address', 'lon', 'lat'
    ]]

    df_failed = df[df['dong_code'].isnull()].copy()
    return df_result, df_failed

if __name__ == "__main__":
    print("[실행] 시장 데이터를 전처리하고 저장합니다...")
    df_raw = load_market_csv()
    df, df_failed = mapping_process(df_raw)

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/processed/market__processed.csv"))
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"저장 완료: {output_path}")

    failures_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/processed/market__failed.csv"))
    try:
        df_failed.to_csv(failures_path, index=False, encoding='utf-8-sig')
        print(f"⚠️ 매핑 실패 데이터 저장 완료: {failures_path}")
    except PermissionError:
        print(f"❌ 실패 파일 저장 실패: {failures_path} - 다른 프로그램에서 열려있는지 확인하세요")