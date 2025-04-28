import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re

# 페이지 기본 설정
st.set_page_config(
    page_title="교환/환불 분석 대시보드",
    layout="wide"
)

def clean_dataframe(df):
    """데이터프레임 정리"""
    if df is None or df.empty:
        return df
        
    # 필수 컬럼 확인
    required_columns = ['모델명', '처리방식', '수량']
    if not all(col in df.columns for col in required_columns):
        return None
        
    # 컬럼 선택 및 데이터 정리
    df = df[required_columns].copy()
    
    # 데이터 정제
    df['모델명'] = df['모델명'].astype(str).str.strip()
    df['처리방식'] = df['처리방식'].astype(str).str.strip()
    df['수량'] = pd.to_numeric(df['수량'], errors='coerce').fillna(0)
    
    # 빈 값이나 숫자만 있는 행, 날짜 형식의 행 제거
    df = df[df['모델명'].str.len() > 0]  # 빈 문자열 제거
    df = df[~df['모델명'].str.match(r'^\d+$')]  # 숫자만 있는 행 제거
    df = df[~df['모델명'].str.contains(r'\d{2,4}[-/]\d{1,2}[-/]\d{1,2}')]  # 날짜 형식 제거
    
    # 특수문자로 시작하는 행 제거
    df = df[~df['모델명'].str.match(r'^[^A-Za-z0-9가-힣]')]
    
    # 처리방식이 유효한 것만 선택
    valid_methods = ['단순변심', '수거하면할', '물량교환', '물량환불', '수거하면 할', 
                    '오배송환불', '오배송교환', '본사교환(오주문)', '택배사사고환불']
    df = df[df['처리방식'].isin(valid_methods)]
    
    # 그룹화하여 합계 계산
    df = df.groupby(['모델명', '처리방식'], as_index=False)['수량'].sum()
    
    # 수량이 0인 행 제거
    df = df[df['수량'] > 0]
    
    return df

def load_google_sheet(spreadsheet_url):
    try:
        # Google Sheets API 인증
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'credentials.json', scope)
        
        gc = gspread.authorize(credentials)
        
        # URL에서 스프레드시트 열기
        sheet = gc.open_by_url(spreadsheet_url)
        
        # 모든 워크시트 처리
        all_sheets = {}
        for worksheet in sheet.worksheets():
            try:
                # 데이터를 데이터프레임으로 변환
                data = worksheet.get_all_values()
                if not data:  # 빈 시트 건너뛰기
                    continue
                    
                headers = data[0]
                values = data[1:]
                
                # 필수 컬럼이 없는 시트 건너뛰기
                if not all(col in headers for col in ['모델명', '처리방식', '수량']):
                    continue
                    
                df = pd.DataFrame(values, columns=headers)
                
                # 데이터 정리
                cleaned_df = clean_dataframe(df)
                if cleaned_df is not None and not cleaned_df.empty:
                    all_sheets[worksheet.title] = cleaned_df
                    
            except Exception as e:
                st.error(f"{worksheet.title} 시트 처리 중 오류 발생: {str(e)}")
                continue
        
        return all_sheets
        
    except Exception as e:
        st.error(f"스프레드시트 로드 중 오류가 발생했습니다: {str(e)}")
        return None

def create_charts(data, title):
    if data is None or data.empty:
        st.error(f"{title}: 처리할 수 있는 데이터가 없습니다.")
        return
        
    st.subheader(f"📊 {title}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("처리방식별 비율")
        process_summary = data.groupby('처리방식')['수량'].sum().reset_index()
        total = process_summary['수량'].sum()
        process_summary['비율'] = (process_summary['수량'] / total * 100).round(1)
        process_summary = process_summary.sort_values('수량', ascending=False)
        
        fig_pie = px.pie(
            process_summary,
            values='수량',
            names='처리방식',
            hole=0.3,
            custom_data=['비율']
        )
        fig_pie.update_traces(
            hovertemplate="처리방식: %{label}<br>건수: %{value}<br>비율: %{customdata[0]}%"
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        st.subheader("상위 10개 모델의 현황")
        top_models = data.groupby('모델명')['수량'].sum().sort_values(ascending=False).head(10)
        fig_bar = px.bar(
            top_models,
            x=top_models.index,
            y='수량',
            labels={'x': '모델명', 'y': '건수'}
        )
        fig_bar.update_traces(
            hovertemplate="모델명: %{x}<br>건수: %{y}"
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    st.subheader("처리방식별 상세 분석")
    
    # 모델별 총 건수 계산
    model_totals = data.groupby('모델명')['수량'].sum()
    top_models = model_totals.sort_values(ascending=False).head(20).index  # 상위 20개 모델만 선택
    
    # 상위 모델에 대한 피벗 테이블 생성
    filtered_data = data[data['모델명'].isin(top_models)]
    process_model = pd.pivot_table(
        filtered_data,
        values='수량',
        index='모델명',
        columns='처리방식',
        fill_value=0
    )
    
    # 합계로 정렬
    process_model['합계'] = process_model.sum(axis=1)
    process_model = process_model.sort_values('합계', ascending=False)
    process_model = process_model.drop('합계', axis=1)
    
    fig_heatmap = px.imshow(
        process_model,
        aspect='auto',
        labels=dict(x='처리방식', y='모델명', color='건수'),
        color_continuous_scale='Blues'
    )
    fig_heatmap.update_traces(
        hovertemplate="모델명: %{y}<br>처리방식: %{x}<br>건수: %{z}"
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)
    
    with st.expander("원본 데이터 보기"):
        st.dataframe(
            data.sort_values(['모델명', '처리방식']),
            column_config={
                "모델명": st.column_config.TextColumn("모델명", width="medium"),
                "처리방식": st.column_config.TextColumn("처리방식", width="medium"),
                "수량": st.column_config.NumberColumn("수량", format="%d")
            }
        )

def main():
    st.title("제품 교환/환불 분석 대시보드")
    
    # 사이드바에 데이터 소스 선택 옵션 추가
    with st.sidebar:
        st.header("데이터 소스 선택")
        data_source = st.radio(
            "데이터 소스를 선택하세요:",
            ["엑셀 파일 업로드", "구글 스프레드시트"]
        )
        
    if data_source == "엑셀 파일 업로드":
        uploaded_file = st.file_uploader("엑셀 파일을 업로드하세요", type=['xlsx', 'xls'])
        
        if uploaded_file is not None:
            try:
                # 엑셀 파일의 모든 시트 읽기
                excel_file = pd.ExcelFile(uploaded_file)
                sheet_names = excel_file.sheet_names
                
                # 각 시트의 데이터 처리
                processed_sheets = {}
                for sheet_name in sheet_names:
                    df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                    cleaned_df = clean_dataframe(df)
                    if cleaned_df is not None and not cleaned_df.empty:
                        processed_sheets[sheet_name] = cleaned_df
                
                if processed_sheets:
                    # 탭 생성
                    tabs = st.tabs(list(processed_sheets.keys()))
                    
                    # 각 시트별로 차트 생성
                    for tab, (sheet_name, data) in zip(tabs, processed_sheets.items()):
                        with tab:
                            create_charts(data, sheet_name)
                else:
                    st.error("처리할 수 있는 데이터가 없습니다. 엑셀 파일의 형식을 확인해주세요.")
                
            except Exception as e:
                st.error(f"데이터를 처리하는 중 오류가 발생했습니다: {str(e)}")
                st.write("엑셀 파일의 형식을 확인해주세요. '모델명', '처리방식', '수량' 컬럼이 필요합니다.")
    
    else:  # 구글 스프레드시트 선택
        spreadsheet_url = st.text_input(
            "구글 스프레드시트 URL을 입력하세요",
            help="공유 설정이 되어 있는 구글 스프레드시트의 URL을 입력하세요."
        )
        
        if spreadsheet_url:
            sheets_data = load_google_sheet(spreadsheet_url)
            if sheets_data:
                # 탭 생성
                tabs = st.tabs(list(sheets_data.keys()))
                
                # 각 시트별 차트 생성
                for tab, (sheet_name, data) in zip(tabs, sheets_data.items()):
                    with tab:
                        create_charts(data, sheet_name)

if __name__ == "__main__":
    main() 