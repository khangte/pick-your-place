# pick-your-place

## 파일구조 예시
```text
📦 pick-your-place/
│
├── 📁 data/
│   ├── raw/                # API/CSV 수집한 원본
│   └── processed/          # 전처리 완료된 동 단위 데이터
│
├── 📁 src/
│   ├── 📁 config/
│   │   └── settings.py          # API key, 경로, 기준값 등
│   │
│   ├── 📁 data_loader/
│   │   ├── marts.py            # 마트/백화점 등 상업시설 API 수집
│   │   ├── cafes.py            # 카페/편의점 등 편의시설
│   │   ├── hospitals.py        # 병원/약국 등
│   │   ├── parks.py            # 공원, 도서관 등
│   │   └── common.py           # 공통 요청 함수 (requests 등)
│   │
│   ├── 📁 geocoding/
│   │   ├── latlon_to_dong.py   # 위도/경도 → 행정동 코드 매핑 함수
│   │   └── dong_mapper.py      # 좌표를 동 코드에 매핑하는 통합 유틸
│   │
│   ├── 📁 preprocessing/
│   │   ├── base_preprocessor.py     # 공통 전처리 클래스/함수
│   │   ├── process_marts.py         # 마트 관련 전처리
│   │   ├── process_cafes.py         # 카페 관련 전처리
│   │   ├── process_hospitals.py     # 병원 관련 전처리
│   │   └── aggregate_by_dong.py     # 모든 데이터를 동 기준으로 병합
│   │
│   ├── 📁 scoring/
│   │   ├── weight_model.py
│   │   └── ml_model.py
│   │
│   ├── 📁 visualization/
│   │   └── map_drawer.py
│   │
│   └── 📁 interface/
│       └── app.py
│
├── .env
├── .ignore
├── requirements.txt
└── README.md
```
