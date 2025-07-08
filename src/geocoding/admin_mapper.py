import re
import pandas as pd

# 행정동 코드 엑셀 불러오기 (최초 1회)
# DONG_CODE_PATH = 'data/reference/KIKcd_H.20250701.xlsx'
MIX_MAPPING_PATH = '../../data/reference/KIKmix_seoul.20250701.csv'

# dong_df = pd.read_excel(DONG_CODE_PATH, dtype=str)
mix_df = pd.read_csv(MIX_MAPPING_PATH, dtype=str)

# 매핑 테이블 정리
mix_df = mix_df.rename(columns={
    "시군구명": "gu_name",
    "동리명": "legal_dong",
    "읍면동명": "admin_dong",
    "행정동코드": "admin_code"
}).dropna(subset=["gu_name", "legal_dong", "admin_dong", "admin_code"])


def extract_gu_and_dong(address: str) -> tuple:
    """
    지번주소 문자열에서 자치구와 법정동 추출 (동/가/로 포함)
    """
    try:
        parts = address.strip().split()
        gu = next((p for p in parts if p.endswith("구")), None)
        dong = next((p for p in parts if p.endswith(("동", "가", "로"))), None)
        return gu, dong
    except Exception as e:
        print(f"[주소 파싱 실패] {address} → {e}")
        return None, None

# def get_gu_dong_codes(gu: str, dong: str) -> tuple:
#     """
#     자치구 + 법정동 기준으로 행정동 이름 → 코드 반환
#     """
#     try:
#         # (0) 이미 행정동이면 바로 코드 조회
#         direct = dong_df[
#             (dong_df["시군구명"] == gu) & (dong_df["읍면동명"] == dong)
#         ]
#         if not direct.empty:
#             dong_code = direct.iloc[0]["행정동코드"]
#             gu_code = dong_code[:5]
#             return gu_code, dong_code
        
#         # (1) 법정동 → 행정동명 매핑
#         match = mix_df[(mix_df["gu_name"] == gu) & (mix_df["legal_dong"] == dong)]

#         if match.empty:
#             print(f"[법정→행정 매핑 실패] gu={gu}, dong={dong}")
#             return None, None

#         admin_dong = match.iloc[0]["admin_dong"]

#         # (2) 행정동명 → 코드 조회
#         code_row = dong_df[
#             (dong_df["시군구명"] == gu) & (dong_df["읍면동명"] == admin_dong)
#         ]

#         if not code_row.empty:
#             dong_code = code_row.iloc[0]["행정동코드"]
#             gu_code = dong_code[:5]
#             return gu_code, dong_code    # <= admin_dong 을 추가로 return을 안해서 정제데이터에 행정동 칼럼이 없었음
#         else:
#             print(f"[행정동 코드 조회 실패] {gu} {admin_dong}")
#             return None, None

#     except Exception as e:
#         print(f"[오류 발생] {e}")
#         return None, None

def get_gu_dong_codes(gu: str, dong: str) -> tuple:
    """
    자치구명(gu)과 동명(dong)을 입력받아 해당하는 자치구코드(gu_code),
    행정동코드(dong_code), 행정동명(admin_dong)을 반환합니다.

    - 입력된 동명이 이미 행정동인 경우: 직접 조회
    - 법정동인 경우: mix 파일을 통해 행정동으로 매핑 후 코드 조회
    """
    try:
        # ① 입력된 동명이 행정동인 경우 → 바로 반환
        direct = mix_df[(mix_df["gu_name"] == gu) & (mix_df["admin_dong"] == dong)]
        if not direct.empty:
            dong_code = direct.iloc[0]["admin_code"]
            gu_code = dong_code[:5]
            admin_dong = direct.iloc[0]["admin_dong"]
            return gu_code, dong_code, admin_dong

        # ② 법정동일 경우 → 행정동으로 매핑 후 반환
        match = mix_df[(mix_df["gu_name"] == gu) & (mix_df["legal_dong"] == dong)]
        if not match.empty:
            dong_code = match.iloc[0]["admin_code"]
            gu_code = dong_code[:5]
            admin_dong = match.iloc[0]["admin_dong"]
            return gu_code, dong_code, admin_dong

        # ③ 실패 시
        print(f"[❌ 매핑 실패] gu={gu}, dong={dong}")
        return None, None, None

    except Exception as e:
        print(f"[⚠️ 예외 발생] gu={gu}, dong={dong} → {e}")
        return None, None, None


def smart_parse_gu_and_dong(address: str):
    """
    주소 문자열에서 자치구, 법정동명을 추출
    - 괄호 안에 동/가 있으면 사용
    - 없으면 전체 주소에서 추출
    - 숫자/번지 제거 포함
    """
    try:
        # ① 자치구 추출
        gu_match = re.search(r"서울특별시\s+(\S+?구)", address)
        gu = gu_match.group(1) if gu_match else None

        dong = None

        # ② 괄호 안에서 동/가로 끝나는 단어 추출
        bracket_match = re.search(r"\(([^)]+)\)", address)
        if bracket_match:
            candidate = bracket_match.group(1)
            # 괄호 안에서 동/가로 끝나는 단어만 허용
            if re.search(r"(동|가)$", candidate):
                dong = candidate.strip()

        # ③ fallback: 괄호 안 실패 시 전체 주소에서 추출
        if not dong:
            # 괄호 제외한 주소에서 동명 찾기
            address_cleaned = re.sub(r"\(.*?\)", "", address)
            after_gu = address_cleaned.split(gu)[-1] if gu else address_cleaned
            tokens = after_gu.strip().split()
            for token in tokens:
                token_clean = re.sub(r"[0-9\-]+.*", "", token)  # 번지 제거
                if token_clean.endswith("동") or token_clean.endswith("가"):
                    dong = token_clean.strip()
                    break

        # ④ 결과 반환
        if gu and dong:
            return gu, dong
        else:
            print(f"[동명 추출 실패] {address}")
            return None, None

    except Exception as e:
        print(f"[파싱 예외] {address} → {e}")
        return None, None

def smart2_parse_gu_and_dong(address: str) -> tuple:
    """
    괄호 안에 동/가 있을 경우 우선 추출, 아니면 전체 주소에서 추출
    괄호 안에 로 는 법정동으로 인식 후 처리
    """
    try:
        gu_match = re.search(r"서울특별시\s+(\S+?구)", address)
        gu = gu_match.group(1) if gu_match else None
        dong = None

        bracket_match = re.search(r"\(([^)]+)\)", address)
        if bracket_match:
            candidates = [c.strip() for c in bracket_match.group(1).split(",")]
            for cand in candidates:
                if cand.endswith(("동", "가")):  # "로"는 법정동으로만 사용
                    dong = cand
                    break

        # fallback: 괄호 안 실패 → 전체 주소에서 추출
        if not dong:
            address_cleaned = re.sub(r"\(.*?\)", "", address)
            after_gu = address_cleaned.split(gu)[-1] if gu else address_cleaned
            tokens = after_gu.strip().split()
            for token in tokens:
                token_clean = re.sub(r"[0-9\-]+.*", "", token)
                if token_clean.endswith(("동", "가")):
                    dong = token_clean.strip()
                    break

        if gu and dong:
            return gu, dong
        else:
            print(f"[동명 추출 실패] {address}")
            return None, None
    except Exception as e:
        print(f"[파싱 예외] {address} → {e}")
        return None, None


def get_gu_code(gu_name: str) -> str:
    """
    자치구 이름을 자치구 코드(5자리)로 변환
    예: '강남구' → '111261'
    """
    try:
        match = mix_df[mix_df["gu_name"] == gu_name]
        if not match.empty:
            admin_code = match.iloc[0]["admin_code"]
            gu_code = admin_code[:5]
            return gu_code
        else:
            print(f"[자치구 코드 매핑 실패] gu_name={gu_name}")
            return None
    except Exception as e:
        print(f"[오류 발생] {gu_name} → {e}")
        return None

from difflib import get_close_matches

def get_gu_and_gu_codes(dong_name: str) -> tuple:
    """
    dong_name(법정동 또는 행정동) 기준으로 가장 유사한 서울시 행정동을 찾아
    (gu_code, dong_code, gu_name)을 반환합니다.

    Parameters:
        dong_name (str): 예: '성수동1가', '충무로2가'

    Returns:
        (gu_code, dong_code, gu_name) or (None, None, None)
    """
    try:
        # 서울특별시 필터링 (이미 필터링된 mix_df를 사용하고 있으므로 생략 가능)
        candidates = mix_df.copy()

        # 후보셋
        admin_dongs = candidates["admin_dong"].dropna().unique()
        legal_dongs = candidates["legal_dong"].dropna().unique()

        # 1️⃣ 행정동 기준 유사 매칭
        match = get_close_matches(dong_name, admin_dongs, n=1, cutoff=0.6)
        if match:
            row = candidates[candidates["admin_dong"] == match[0]].iloc[0]
            return row["admin_code"][:5], row["admin_code"], row["gu_name"]

        # 2️⃣ 법정동 기준 유사 매칭 → 해당 행정동으로 다시 매핑
        match = get_close_matches(dong_name, legal_dongs, n=1, cutoff=0.6)
        if match:
            filtered = candidates[candidates["legal_dong"] == match[0]]
            if not filtered.empty:
                row = filtered.iloc[0]
                return row["admin_code"][:5], row["admin_code"], row["gu_name"]

        print(f"[동명 매핑 실패] dong_name={dong_name}")
        return None, None, None

    except Exception as e:
        print(f"[오류 발생] {dong_name} → {e}")
        return None, None, None
    