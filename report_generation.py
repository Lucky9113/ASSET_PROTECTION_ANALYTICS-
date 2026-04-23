import pandas as pd
import sqlite3
import streamlit as st
from sklearn.ensemble import IsolationForest
import google.generativeai as genai
import os
from dotenv import load_dotenv

# --- 1. LOAD CONFIGURATION ---
load_dotenv()  # This looks for the .env file in the current folder
api_key = os.getenv("GEMINI_API_KEY")

if api_key:
    genai.configure(api_key=api_key)

# --- 2. STREAMLIT CONFIG & UI ---
st.set_page_config(page_title="AI Loss Prevention Pro", layout="wide")
st.title("Enterprise LP Intelligence System")

with st.sidebar:

    st.write("---")
    st.header("Global Filters")
    min_loss = st.sidebar.slider("Min. Transaction Value ($)", 0, 500, 20)

# --- 3. ADVANCED DATA EXTRACTION (Your 4 Patterns) ---


def get_analyzed_data():
    try:
        conn = sqlite3.connect('pos_data.db')

        # Patterns 1, 3, 4
        query_individual = """
        SELECT *, 'Direct Theft' as Category 
        FROM POS_Transactions 
        WHERE (Post_Payment_Void = 1)
           OR (Override_Flag = 1 AND (Net_Amount / Gross_Amount) < 0.20 AND Time_To_Scan_Sec < 3.0)
           OR (Transaction_Type = 'Return' AND Presence_Score < 0.20)
        """
        df_indiv = pd.read_sql(query_individual, conn)

        # Pattern 2: Aggregated Discount Abuse
        query_agg = """
        SELECT Employee_ID FROM POS_Transactions
        WHERE Discount_Type = 'Employee'
        GROUP BY Employee_ID
        HAVING COUNT(*) > 20 AND SUM(Gross_Amount - Net_Amount) > 1000
        """
        abuse_list = pd.read_sql(query_agg, conn)['Employee_ID'].tolist()

        if abuse_list:
            placeholders = ', '.join(['?'] * len(abuse_list))
            query_p2 = f"SELECT *, 'Discount Abuse' as Category FROM POS_Transactions WHERE Employee_ID IN ({placeholders}) AND Discount_Type = 'Employee'"
            df_p2 = pd.read_sql(query_p2, conn, params=abuse_list)
        else:
            df_p2 = pd.DataFrame()

        df_final = pd.concat([df_indiv, df_p2]).drop_duplicates(
            subset=['Transaction_ID'])
        df_final['Timestamp'] = pd.to_datetime(df_final['Timestamp'])
        conn.close()
        return df_final
    except Exception as e:
        st.error("Database connection failed. Run the generator script first!")
        return pd.DataFrame()


# --- 4. EXECUTION & AI PRIORITIZATION ---
df_suspicious = get_analyzed_data()

if not df_suspicious.empty:
    df_filtered = df_suspicious[df_suspicious['Gross_Amount'] >= min_loss]

    if not df_filtered.empty:
        model = IsolationForest(contamination=0.02, random_state=42)
        features = ['Gross_Amount', 'Net_Amount',
                    'Time_To_Scan_Sec', 'Presence_Score']
        df_filtered['AI_Outlier'] = model.fit_predict(df_filtered[features])

        df_priority = df_filtered[df_filtered['AI_Outlier'] == -1].copy()
        df_priority['Urgency_Rank'] = (
            0.95 * df_priority['Gross_Amount']).round(2)
        df_display = df_priority.sort_values('Urgency_Rank', ascending=False)
    else:
        df_display = pd.DataFrame()

    # --- 5. THE GEMINI CHAT LAYER ---
    st.write("---")
    st.subheader("Ask LP Consultant")

    # Use a form so pressing 'Enter' or clicking 'Ask' triggers it once
    with st.form("ai_chat"):
        user_question = st.text_input(
            "Ask about patterns (e.g., 'Analyze the top 3 risks')")
        submitted = st.form_submit_button("Ask Gemini")
    if submitted and user_question:
        if not api_key:
            st.error("Missing API Key. Check your .env file.")
        elif not df_display.empty:
            # 1. Create the context summary
            summary = f"""
            LP Data Summary:
            - Flagged Incidents: {len(df_display)}
            - Total Risk Value: ${df_display['Gross_Amount'].sum():,.2f}
            - Most Active Suspect: {df_display['Employee_ID'].value_counts().idxmax() if not df_display.empty else "None"}
            - Sample Records: {df_display[['Employee_ID', 'Category', 'Gross_Amount']].head(5).to_string()}
            """
            try:
                model = genai.GenerativeModel('gemini-2.5-flash')
                with st.spinner("AI is thinking... (Respecting API Rate Limits)"):
                    response = model.generate_content(
                        f"You are a Retail Loss Prevention Expert. Context: {summary}. Question: {user_question}"
                    )
                    # Store in session state so the answer doesn't disappear on next refresh
                    st.session_state['ai_answer'] = response.text

            except Exception as e:
                if "429" in str(e):
                    st.error(
                        "Quota Limit Hit: Please wait 60 seconds before asking another question.")
                else:
                    st.error(f"AI Error: {e}")

   # --- 5.1 DISPLAY THE ANSWER ---
    if 'ai_answer' in st.session_state:
        st.info(f"**Gemini Analysis:**\n\n{st.session_state['ai_answer']}")
    # --- 6. VISUAL DASHBOARD ---
    st.write("---")
    st.subheader("Investigation Dashboard")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Anomalies", len(df_display))
    m2.metric("Risk Value", f"${df_display['Gross_Amount'].sum():,.2f}")
    m3.metric("Avg. Theft",
              f"${df_display['Gross_Amount'].mean():,.2f}" if not df_display.empty else "$0")
    m4.metric("Unique Suspects", df_display['Employee_ID'].nunique(
    ) if not df_display.empty else 0)

    st.dataframe(df_display[['Urgency_Rank', 'Category', 'Employee_ID',
                 'Gross_Amount', 'Timestamp', 'Item_SKU']], use_container_width=True)
else:
    st.warning(
        "No data found. Please run your data generation script to create pos_data.db.")
