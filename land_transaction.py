import os
import json
import time
import requests
import pandas as pd
from tabulate import tabulate

proxies = {
    "http": "http://nwproxy.ahnlab.co.kr:3128",
    "https": "http://nwproxy.ahnlab.co.kr:3128",
}

KAKAO_REST_API_KEY = 'c17e2023ed285cc18c0b8ddf195f478c'
ADDRESS_FILE = "address_data.json"
PERMISSION_LIST_FILE = "permission_list.csv"
PERMISSION_LIST_DEDUP_FILE = "permission_list_dedup.csv"

# 서울시 자치구 코드 상수
SEOCHO_GU = "11650"  # 서초구
GANGNAM_GU = "11680"  # 강남구
SONGPA_GU = "11710"   # 송파구

# 주요 컬럼 한글 이름 설정
COLUMN_RENAME = {
    "HNDL_YMD": "허가일자",
    "JOB_GBN_NM": "구분",
    "USE_PURP": "용도",
    "ADDRESS": "주소",
    "APT_NAME": "아파트명",
    "ACC_NO": "일련번호"
}

# 주소 데이터 불러오기
def load_address_data():
    if os.path.exists(ADDRESS_FILE):
        try:
            with open(ADDRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


# 주소 데이터 저장
def save_address_data(address_data):
    try:
        with open(ADDRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(address_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"주소 저장 실패: {e}")


def get_building_name_from_kakao(address):
    # 너무 빠른 요청 방지
    time.sleep(0.1)
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {
        "Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"
    }
    params = {
        "query": address
    }

    try:
        response = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=10)
        response.raise_for_status()
        result = response.json()

        documents = result.get("documents", [])
        if not documents:
            return "주소 검색 실패"

        building_name = documents[0].get("road_address", {}).get("building_name", "건물명 없음")
        return building_name
    except Exception as e:
        return f"오류: {e}"


def get_building_name(address_data, address):
    name = address_data.get(address)
    if name is not None:
        return name
    else:
        name = get_building_name_from_kakao(address)
        address_data[address] = name
        return name

def enrich_with_building_name(df):
    address_data = load_address_data()

    def enrich(row):
        raw_addr = row["ADDRESS"].strip()
        full_addr = "서울특별시 " + raw_addr
        building_name = get_building_name(address_data, full_addr)

        dong = raw_addr.split()[1] if len(raw_addr.split()) >= 2 else ""
        apt_name = f"{dong} {building_name}" if building_name and dong else building_name

        return pd.Series({"DONG_NAME": dong, "APT_NAME": apt_name})

    df[["DONG_NAME", "APT_NAME"]] = df.apply(enrich, axis=1)
    save_address_data(address_data)
    return df

def deduplicate_by_acc_no(df):
    seen = set()
    dedup_rows = []

    for _, row in df.iterrows():
        acc_no = row["ACC_NO"]

        if acc_no in seen:
            continue  # 이미 처리한 ACC_NO는 건너뜀

        # 동일한 ACC_NO를 가진 모든 row 추출
        group = df[df["ACC_NO"] == acc_no]

        # APT_NAME이 유효한 row 선택
        valid_group = group[
            ~group["APT_NAME"].str.contains("오류|검색 실패|건물명 없음", na=False)
        ]

        if not valid_group.empty:
            # 유효한 row 중 첫 번째를 사용
            chosen_row = valid_group.iloc[0]
        else:
            # 모두 실패한 경우 그냥 첫 번째 row 사용
            chosen_row = group.iloc[0]

        dedup_rows.append(chosen_row)
        seen.add(acc_no)

    # 리스트를 DataFrame으로 변환
    return pd.DataFrame(dedup_rows).reset_index(drop=True)


def fetch_land_transaction_permits(sgg_code, begin_date, end_date):
    """
        서울시 토지거래허가 내역 조회 함수

       Args:
           sgg_code (str): 자치구 코드 (예: 서초구 "11650")
           begin_date (str): 시작일 (예: "20250425")
           end_date (str): 종료일 (예: "20250625")
       """

    url = "https://land.seoul.go.kr/land/wsklis/getContractList.do"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://land.seoul.go.kr",
        "Referer": "https://land.seoul.go.kr/land/other/contractStatus.do",
        "X-Requested-With": "XMLHttpRequest"
    }

    payload = {
        "sggCd": sgg_code,
        "beginDate": begin_date,
        "endDate": end_date
    }

    response = requests.post(url, data=payload, headers=headers, proxies=proxies, timeout=10)
    response.raise_for_status()

    json_data = response.json()
    rows = json_data.get("result", [])
    if not rows:
        print("❌ 수집된 데이터가 없습니다.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df[df["USE_PURP"] == "주거용"].reset_index(drop=True)
    df["HNDL_YMD"] = pd.to_datetime(df["HNDL_YMD"], format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def show_apartment_summary(df, building_name: str):
    # 필터링
    result = df[df["APT_NAME"] == building_name]

    if result.empty:
        print(f"\n❌ 해당 아파트를 찾을 수 없습니다: '{building_name}'")
        return

    result = result[list(COLUMN_RENAME.keys())].rename(columns=COLUMN_RENAME)

    # 표 출력 (grid → simple 로 구분선 제거)
    print(f"\n🏢 '{building_name}' 아파트 상세 정보:")
    print(tabulate(result, headers='keys', tablefmt='simple', showindex=False))


def show_apt_by_dong(df, dong_name: str):
    # 해당 동 필터링
    filtered_df = df[df["DONG_NAME"] == dong_name]

    if filtered_df.empty:
        print(f"❌ 해당 동의 데이터가 없습니다: {dong_name}")
        return

        # 동 내 건물명 추출
    building_names = filtered_df["APT_NAME"].unique()

    print(f"\n🏘️ '{dong_name}' 내 아파트 정보 (총 {len(building_names)}개 건물)\n")

    for building_name in building_names:
        result = filtered_df[filtered_df["APT_NAME"] == building_name]

        if result.empty:
            continue

        result = result[list(COLUMN_RENAME.keys())].rename(columns=COLUMN_RENAME)

        print(f"\n🏢 {building_name}")
        print(tabulate(result, headers='keys', tablefmt='simple', showindex=False))


def summary_dong(df):
    summary = df.groupby(["DONG_NAME"]).size().reset_index(name="COUNT")
    summary = summary.sort_values(by="COUNT", ascending=False).reset_index(drop=True)  # ← 정렬 추가
    print("\n📊 동별 상세 통계:")
    print(summary)


def summary_apt(df):
    summary = df.groupby(["DONG_NAME", "APT_NAME"]).size().reset_index(name="COUNT")
    summary = summary.sort_values(by="COUNT", ascending=False).reset_index(drop=True)  # ← 정렬 추가
    print("\n📊 동별 아파트 상세 통계:")
    print(summary)



# 실행
if __name__ == "__main__":
    # 출력 설정: 행/열 제한 해제
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)

    df = fetch_land_transaction_permits(SEOCHO_GU, "20250625", "20250626")
    if df.empty:
        exit(0)

    df.to_csv(PERMISSION_LIST_FILE, index=False, encoding="utf-8-sig")

    df = enrich_with_building_name(df)
    df = deduplicate_by_acc_no(df)

    df.to_csv(PERMISSION_LIST_DEDUP_FILE, index=False, encoding="utf-8-sig")
    print("✅ 중복 제거 후 CSV 저장 완료")

    print(df.head())
    summary_dong(df)
    summary_apt(df)
    show_apt_by_dong(df, '우면동')
    show_apt_by_dong(df, '양재동')

