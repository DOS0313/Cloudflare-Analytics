import os
import json
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.oauth2 import service_account
from pathlib import Path


class ConfigHandler:
    def __init__(self, config_path="config.json"):
        self.config_path = config_path
        self.config = self.load_config()

    def load_config(self):
        """설정 파일 로드"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            self.validate_config(config)
            return config
        except FileNotFoundError:
            raise Exception(f"설정 파일을 찾을 수 없습니다: {self.config_path}")
        except json.JSONDecodeError:
            raise Exception(f"설정 파일 형식이 잘못되었습니다: {self.config_path}")

    def validate_config(self, config):
        """필수 설정값 확인"""
        required_fields = {
            'cloudflare': ['api_token', 'zone_id'],
            'google_sheets': ['credentials_file', 'spreadsheet_id', 'sheet_name']
        }

        for section, fields in required_fields.items():
            if section not in config:
                raise Exception(f"설정 파일에 '{section}' 섹션이 없습니다.")

            for field in fields:
                if field not in config[section]:
                    raise Exception(f"설정 파일의 '{section}' 섹션에 '{field}' 필드가 없습니다.")

                if not config[section][field]:
                    raise Exception(f"설정 파일의 '{section}.{field}' 값이 비어있습니다.")


class CloudflareAnalytics:
    def __init__(self, config):
        self.headers = {
            'Authorization': f'Bearer {config["cloudflare"]["api_token"]}',
            'Content-Type': 'application/json'
        }
        self.zone_id = config["cloudflare"]["zone_id"]
        self.base_url = 'https://api.cloudflare.com/client/v4'
        self.graphql_url = 'https://api.cloudflare.com/client/v4/graphql'

    def get_last_30days_analytics(self):
        """최근 30일간의 일별 Analytics 데이터 수집"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        # 날짜 형식 변환
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        print(f"\n수집 기간: {start_date_str} ~ {end_date_str}")

        query = """
        query AnalyticsData($zoneTag: String!, $start: Date!, $end: Date!) {
          viewer {
            zones(filter: { zoneTag: $zoneTag }) {
              httpRequests1dGroups(
                limit: 100,
                filter: { date_geq: $start, date_leq: $end }
              ) {
                dimensions {
                  date
                }
                sum {
                  bytes
                  cachedBytes
                  requests
                  cachedRequests
                  pageViews
                  threats
                }
                uniq {
                  uniques
                }
              }
            }
          }
        }
        """

        variables = {
            "zoneTag": self.zone_id,
            "start": start_date_str,
            "end": end_date_str
        }

        try:
            response = requests.post(
                self.graphql_url,
                headers=self.headers,
                json={"query": query, "variables": variables}
            )

            if response.status_code != 200:
                print(f"API 응답 상태 코드: {response.status_code}")
                print(f"응답 내용: {response.text}")
                raise Exception(f"API 요청 실패: {response.status_code}")

            data = response.json()

            if 'errors' in data and data['errors']:
                print(f"GraphQL 응답: {json.dumps(data, indent=2)}")
                raise Exception(f"GraphQL 에러: {data['errors']}")

            zones_data = data.get('data', {}).get('viewer', {}).get('zones', [])
            if not zones_data:
                raise Exception("응답 데이터에 zones 정보가 없습니다.")

            requests_data = zones_data[0].get('httpRequests1dGroups', [])
            if not requests_data:
                print("주의: 지정된 기간에 데이터가 없습니다.")
                return None

            # 일별 데이터 처리
            daily_data = []

            for day in requests_data:
                dimensions = day['dimensions']
                sum_data = day['sum']
                uniq_data = day['uniq']

                # 일별 데이터 생성
                daily_record = {
                    '날짜': dimensions['date'],
                    '고유 방문자': uniq_data.get('uniques', 0),
                    '페이지뷰': sum_data.get('pageViews', 0),
                    '총 요청수': sum_data.get('requests', 0),
                    '캐시된 요청수': sum_data.get('cachedRequests', 0),
                    '총 데이터(bytes)': sum_data.get('bytes', 0),
                    '캐시된 데이터(bytes)': sum_data.get('cachedBytes', 0),
                    '위협 감지': sum_data.get('threats', 0)
                }

                # 캐시 비율 계산
                total_requests = daily_record['총 요청수']
                if total_requests > 0:
                    cache_ratio = (daily_record['캐시된 요청수'] / total_requests) * 100
                    daily_record['캐시 비율(%)'] = round(cache_ratio, 2)
                else:
                    daily_record['캐시 비율(%)'] = 0

                daily_data.append(daily_record)

            # 날짜순으로 정렬
            daily_data.sort(key=lambda x: x['날짜'], reverse=True)  # 최신 날짜가 위로 오도록 정렬

            return daily_data

        except Exception as e:
            print(f"데이터 수집 중 오류 발생: {str(e)}")
            raise


class GoogleSheetHandler:
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def __init__(self, config):
        credentials_path = config['google_sheets']['credentials_file']
        self.spreadsheet_id = config['google_sheets']['spreadsheet_id']
        self.sheet_name = config['google_sheets']['sheet_name']

        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path, scopes=self.SCOPES)
            self.service = build('sheets', 'v4', credentials=self.credentials)
            self.sheet = self.service.spreadsheets()
        except Exception as e:
            raise Exception(f"Google Sheets 인증 실패: {str(e)}")

    def format_bytes(self, bytes_value):
        """바이트 값을 읽기 쉬운 형식으로 변환"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"

    def get_existing_data(self):
        """스프레드시트의 현재 데이터 조회"""
        try:
            result = self.sheet.values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f'{self.sheet_name}!A:J'
            ).execute()

            return result.get('values', [])
        except Exception as e:
            print(f"기존 데이터 조회 중 오류 발생: {str(e)}")
            return []

    def append_daily_data(self, daily_data):
        """일별 데이터를 스프레드시트에 추가"""
        if not daily_data:
            print("추가할 데이터가 없습니다.")
            return None

        try:
            # 기존 데이터 조회
            existing_data = self.get_existing_data()

            # 헤더가 없는 경우 헤더 추가
            headers = [
                '날짜', '고유 방문자', '페이지뷰', '총 요청수', '캐시된 요청수',
                '캐시 비율(%)', '총 데이터', '캐시된 데이터', '위협 감지'
            ]

            if not existing_data:
                existing_data = [headers]

            # 기존 데이터의 날짜 목록 생성
            existing_dates = set()
            if len(existing_data) > 1:  # 헤더를 제외한 데이터가 있는 경우
                existing_dates = {row[0] for row in existing_data[1:]}

            # 날짜순으로 정렬 (오래된 날짜가 위로)
            daily_data.sort(key=lambda x: x['날짜'])

            # 새로운 데이터만 필터링
            new_rows = []
            duplicate_count = 0

            for day in daily_data:
                # 날짜 형식 변환: YYYY-MM-DD를 DD/MM/YYYY 형식으로 변환
                date_obj = datetime.strptime(day['날짜'], '%Y-%m-%d')
                formatted_date = f"=DATE({date_obj.year}, {date_obj.month}, {date_obj.day})"

                if day['날짜'] not in existing_dates:
                    row = [
                        formatted_date,  # 날짜 형식 지정
                        day['고유 방문자'],
                        day['페이지뷰'],
                        day['총 요청수'],
                        day['캐시된 요청수'],
                        day['캐시 비율(%)'],
                        self.format_bytes(day['총 데이터(bytes)']),
                        self.format_bytes(day['캐시된 데이터(bytes)']),
                        day['위협 감지']
                    ]
                    new_rows.append(row)
                else:
                    duplicate_count += 1

            if not new_rows:
                print("추가할 새로운 데이터가 없습니다.")
                if duplicate_count > 0:
                    print(f"중복된 데이터: {duplicate_count}개")
                return None

            # 새 데이터 추가
            result = self.sheet.values().append(
                spreadsheetId=self.spreadsheet_id,
                range=f'{self.sheet_name}!A1',
                valueInputOption='USER_ENTERED',  # 수식을 해석하도록 설정
                insertDataOption='INSERT_ROWS',
                body={'values': new_rows}
            ).execute()

            # 날짜 열 서식 지정 (선택사항)
            sheet_id = self.get_sheet_id()
            if sheet_id:
                date_format_request = {
                    'requests': [
                        {
                            'repeatCell': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startColumnIndex': 0,
                                    'endColumnIndex': 1,
                                    'startRowIndex': 1  # 헤더 제외
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'numberFormat': {
                                            'type': 'DATE',
                                            'pattern': 'yyyy-mm-dd'
                                        }
                                    }
                                },
                                'fields': 'userEnteredFormat.numberFormat'
                            }
                        }
                    ]
                }
                self.sheet.batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=date_format_request
                ).execute()

            print(f"새로 추가된 데이터: {len(new_rows)}개")
            if duplicate_count > 0:
                print(f"중복된 데이터: {duplicate_count}개")

            return result

        except Exception as e:
            print(f"데이터 추가 중 오류 발생: {str(e)}")
            raise

    def get_sheet_id(self):
        """현재 시트의 ID 조회"""
        try:
            spreadsheet = self.sheet.get(
                spreadsheetId=self.spreadsheet_id
            ).execute()

            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == self.sheet_name:
                    return sheet['properties']['sheetId']
            return None
        except Exception:
            return None


def should_collect_data():
    """데이터 수집이 필요한지 확인"""
    now = datetime.now()
    print(f"현재 날짜: {now.day}")
    return now.day == 1

def main():
    print("Cloudflare Analytics 서비스 시작...")
    print(f"현재 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(" ")
    
    try:
        script_dir = Path(__file__).parent
        config_path = script_dir / "config.json"
        
        print("설정 정보")
        config_handler = ConfigHandler(config_path)
        config = config_handler.config
        
        print(f"- Config 파일 경로: {config_path}")
        print(f"- Cloudflare Zone ID: {config['cloudflare']['zone_id']}")
        print(f"- Spreadsheet ID: {config['google_sheets']['spreadsheet_id']}")
        print(f"- Sheet 이름: {config['google_sheets']['sheet_name']}")
        print(f"- Credentials 파일: {config['google_sheets']['credentials_file']}")
        print(" ")
    
        while True:
            try:
                if should_collect_data():
                    print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 월간 데이터 수집 시작")
                    
                    print("Cloudflare 데이터 수집 중...")
                    cf_analytics = CloudflareAnalytics(config)

                    # 최근 30일 데이터 수집
                    daily_data = cf_analytics.get_last_30days_analytics()
                    if not daily_data:
                        print("수집할 데이터가 없습니다.")
                    else:
                        print("\nGoogle Spreadsheet에 데이터 추가 중...")
                        gsheet = GoogleSheetHandler(config)

                        result = gsheet.append_daily_data(daily_data)
                        if result:
                            print(f"\n데이터가 성공적으로 추가되었습니다.")

                            # 새로 추가된 데이터의 통계 요약
                            total_requests = sum(day['총 요청수'] for day in daily_data)
                            total_cached = sum(day['캐시된 요청수'] for day in daily_data)
                            total_visitors = sum(day['고유 방문자'] for day in daily_data)
                            total_bytes = sum(day['총 데이터(bytes)'] for day in daily_data)
                            total_cached_bytes = sum(day['캐시된 데이터(bytes)'] for day in daily_data)

                            print("\n수집 데이터 요약:")
                            print(f"수집 기간: {daily_data[0]['날짜']} ~ {daily_data[-1]['날짜']}")
                            print(f"총 고유 방문자: {total_visitors:,}")
                            print(f"총 요청 수: {total_requests:,}")
                            print(f"총 캐시된 요청 수: {total_cached:,}")

                            if total_requests > 0:
                                cache_ratio = (total_cached / total_requests) * 100
                                print(f"전체 캐시 비율: {cache_ratio:.2f}%")

                            print(f"\n총 데이터: {gsheet.format_bytes(total_bytes)}")
                            print(f"캐시된 데이터: {gsheet.format_bytes(total_cached_bytes)}")

                            if total_bytes > 0:
                                bytes_cache_ratio = (total_cached_bytes / total_bytes) * 100
                                print(f"데이터 캐시 비율: {bytes_cache_ratio:.2f}%")

                    print("\n다음 데이터 수집까지 대기 중...")
                
                # 1시간마다 체크
                time.sleep(3600)

            except Exception as e:
                print(f"\n에러 발생: {str(e)}")
                print("1시간 후 다시 시도합니다...")
                time.sleep(3600)

    except Exception as e:
        print(f"\n에러 발생: {str(e)}")
        print("1시간 후 다시 시도합니다...")
        time.sleep(3600)

if __name__ == "__main__":
    main()