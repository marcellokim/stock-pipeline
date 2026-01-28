import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

# 1. DB 연결 (Docker Compose 설정과 일치)
DB_URL = "postgresql://dev_user:dev_password@localhost:5432/stock_db"
engine = create_engine(DB_URL)

def collect_stock_data(ticker_symbol):
    print(f"📈 {ticker_symbol} 데이터 수집 중...")

    # [수정] yf.download 대신 Ticker 객체의 history 사용
    # 이유: download는 MultiIndex(튜플)를 반환해 처리가 복잡함
    ticker = yf.Ticker(ticker_symbol)
    df = ticker.history(period="5d")

    if df.empty:
        print(f"❌ {ticker_symbol} 데이터를 찾을 수 없습니다.")
        return

    # 데이터 가공
    df = df.reset_index()

    # [수정] 컬럼 정리 (이제 컬럼이 단순 문자열이므로 lower()가 작동함)
    # 공백 제거 및 소문자 변환 ('Stock Splits' -> 'stock_splits' 등 방지)
    df.columns = [str(col).lower().replace(' ', '_') for col in df.columns]

    # 타임존 정보 제거 (PostgreSQL 저장 시 호환성 문제 방지)
    if 'date' in df.columns:
        df['date'] = df['date'].dt.tz_localize(None)

    df['ticker'] = ticker_symbol

    # 2. DB에 저장
    try:
        df.to_sql('daily_stocks', engine, if_exists='append', index=False)
        print(f"✅ {ticker_symbol} 저장 완료! ({len(df)}건)")
    except Exception as e:
        print(f"❌ DB 저장 실패: {e}")

if __name__ == "__main__":
    # 삼성전자, 테슬라
    stocks = ["005930.KS", "TSLA"]
    for s in stocks:
        collect_stock_data(s)