import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import io
from azure.storage.blob import BlobServiceClient
import openai
import json
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

# Configuration
STORAGE_ACCOUNT = "aienablementstorage1"
CONTAINER = "test"
STORAGE_KEY = "QIhpMhK6lzlA2trKESzZxW/x7gu13JeMNSrz8ZfpFrVE+dDd2bQ7gU3zylj46Sfr65SF80+0ocWW+ASt5jpSYw=="

class IntelligentDataAgent:
    def __init__(self, openai_api_key):
        """Initialize the intelligent agent with OpenAI API"""
        self.openai_client = openai.OpenAI(api_key=openai_api_key)
        self.data = {}
        self.analysis_cache = {}
        
    def load_data_from_adls(self):
        """Load all 7 sheets from ADLS"""
        try:
            conn_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};AccountKey={STORAGE_KEY};EndpointSuffix=core.windows.net"
            service_client = BlobServiceClient.from_connection_string(conn_str)
            container_client = service_client.get_container_client(CONTAINER)
            
            blob_client = container_client.get_blob_client("Envista clean dataset.xlsx")
            stream = io.BytesIO(blob_client.download_blob().readall())
            
            # Load all sheets
            all_sheets = pd.read_excel(stream, sheet_name=None, engine='openpyxl')
            self.data = all_sheets
            
            # Create comprehensive data summary for the AI
            self.data_summary = self._create_data_summary()
            
            return f"Successfully loaded {len(all_sheets)} sheets: {list(all_sheets.keys())}"
            
        except Exception as e:
            return f"Error loading data: {str(e)}"
    
    def _create_data_summary(self):
        """Create comprehensive summary of all datasets for AI understanding"""
        summary = {
            "sheets": {},
            "overall_structure": {
                "total_sheets": len(self.data),
                "sheet_names": list(self.data.keys())
            }
        }
        
        for sheet_name, df in self.data.items():
            summary["sheets"][sheet_name] = {
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
                "data_types": df.dtypes.to_dict(),
                "sample_data": df.head(3).to_dict(),
                "missing_values": df.isnull().sum().to_dict(),
                "numeric_columns": list(df.select_dtypes(include=[np.number]).columns),
                "categorical_columns": list(df.select_dtypes(include=['object']).columns)
            }
            
            # Identify potential key columns for manufacturing
            manufacturing_patterns = ['BEGQTY', 'ENDQTY', 'OPER', 'NODE', 'YIELD', 'DEFECT', 'BATCH', 'LOT']
            key_columns = [col for col in df.columns 
                          if any(pattern in str(col).upper() for pattern in manufacturing_patterns)]
            summary["sheets"][sheet_name]["key_manufacturing_columns"] = key_columns
        
        return summary
    
    def ask_agent(self, user_question):
        """Main method to process user questions using OpenAI"""
        
        # Create context for the AI about the data
        data_context = f"""
        You are an expert manufacturing data analyst with access to a comprehensive dataset with {len(self.data)} sheets:
        
        DATA STRUCTURE:
        {json.dumps(self.data_summary, indent=2, default=str)}
        
        CAPABILITIES:
        - Feature importance analysis using Random Forest
        - Anomaly detection using Isolation Forest
        - Time series analysis and forecasting
        - Yield optimization analysis
        - Process parameter correlation analysis
        - Defect prediction and root cause analysis
        - Statistical analysis and hypothesis testing
        - Advanced visualizations
        
        USER QUESTION: {user_question}
        
        Instructions:
        1. Analyze the user's question and determine what type of analysis is needed
        2. Identify which sheet(s) and columns are most relevant
        3. Provide a detailed analysis plan
        4. Generate Python code to execute the analysis
        5. Interpret results and provide actionable insights
        
        Return your response as JSON with these keys:
        - "analysis_type": type of analysis needed
        - "relevant_sheets": which sheets to use
        - "relevant_columns": key columns to analyze
        - "analysis_plan": step-by-step plan
        - "python_code": executable Python code for the analysis
        - "interpretation": how to interpret the results
        - "business_insights": actionable business recommendations
        """
        
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert manufacturing data analyst. Always respond with valid JSON."},
                    {"role": "user", "content": data_context}
                ],
                temperature=0.1
            )
            
            ai_response = json.loads(response.choices[0].message.content)
            return ai_response
            
        except Exception as e:
            return {
                "error": f"AI processing error: {str(e)}",
                "analysis_type": "error_handling",
                "python_code": "# Error occurred in AI processing"
            }
    
    def execute_analysis(self, ai_response, user_question):
        """Execute the analysis code generated by AI"""
        try:
            # Create a safe execution environment
            exec_globals = {
                'pd': pd,
                'np': np,
                'plt': plt,
                'sns': sns,
                'px': px,
                'go': go,
                'make_subplots': make_subplots,
                'RandomForestRegressor': RandomForestRegressor,
                'IsolationForest': IsolationForest,
                'LabelEncoder': LabelEncoder,
                'train_test_split': train_test_split,
                'mean_squared_error': mean_squared_error,
                'r2_score': r2_score,
                'data': self.data,
                'st': st,
                'results': {}
            }
            
            # Execute the AI-generated code
            exec(ai_response.get('python_code', ''), exec_globals)
            
            return exec_globals.get('results', {}), exec_globals.get('fig', None)
            
        except Exception as e:
            return {"error": f"Execution error: {str(e)}"}, None
    
    def generate_predefined_insights(self):
        """Generate predefined insights for Surya's questions"""
        insights = {}
        
        # Try to find the main manufacturing sheet
        main_sheet = None
        for sheet_name, df in self.data.items():
            if any(col in str(sheet_name).lower() for col in ['2024', 'mar', 'main', 'data']):
                main_sheet = df
                break
        
        if main_sheet is None:
            main_sheet = list(self.data.values())[0]  # Use first sheet as fallback
        
        # 1. Key Variables Analysis
        if 'ENDQTY950' in main_sheet.columns and 'BEGQTY200' in main_sheet.columns:
            main_sheet['overall_yield'] = main_sheet['ENDQTY950'] / main_sheet['BEGQTY200']
            
            # Feature importance
            feature_cols = [col for col in main_sheet.columns if any(pattern in str(col).upper() 
                           for pattern in ['OPER', 'NODE', 'BEGQTY', 'ENDQTY'])]
            
            if feature_cols and len(feature_cols) > 0:
                # Prepare data for ML
                analysis_df = main_sheet[feature_cols + ['overall_yield']].copy()
                analysis_df = analysis_df.select_dtypes(include=[np.number]).fillna(0)
                
                if len(analysis_df.columns) > 1:
                    X = analysis_df.drop('overall_yield', axis=1, errors='ignore')
                    y = analysis_df['overall_yield'].fillna(analysis_df['overall_yield'].median())
                    
                    rf = RandomForestRegressor(n_estimators=100, random_state=42)
                    rf.fit(X, y)
                    
                    feature_importance = pd.DataFrame({
                        'variable': X.columns,
                        'importance': rf.feature_importances_
                    }).sort_values('importance', ascending=False)
                    
                    insights['feature_importance'] = feature_importance.head(10)
        
        # 2. Optimal Operating Ranges
        if 'overall_yield' in main_sheet.columns:
            high_yield_lots = main_sheet[main_sheet['overall_yield'] >= main_sheet['overall_yield'].quantile(0.9)]
            insights['high_yield_count'] = len(high_yield_lots)
            insights['total_lots'] = len(main_sheet)
        
        # 3. Anomaly Detection
        numeric_cols = main_sheet.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 1:
            iso_forest = IsolationForest(contamination=0.1, random_state=42)
            anomalies = iso_forest.fit_predict(main_sheet[numeric_cols].fillna(0))
            insights['anomalies_detected'] = (anomalies == -1).sum()
        
        return insights

def main():
    st.set_page_config(page_title="Manufacturing Intelligence Agent", layout="wide", page_icon="🤖")
    
    st.title("🤖 Intelligent Manufacturing Data Agent")
    st.subheader("Ask me anything about your manufacturing data - I'll analyze and provide insights automatically")
    
    # Initialize session state
    if 'agent' not in st.session_state:
        st.session_state.agent = None
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        openai_key = st.text_input("OpenAI API Key", type="password", 
                                  help="Enter your OpenAI API key to enable intelligent analysis")
        
        if openai_key and not st.session_state.agent:
            st.session_state.agent = IntelligentDataAgent(openai_key)
            
        if st.button("Load Data from ADLS"):
            if st.session_state.agent:
                with st.spinner("Loading data from Azure Data Lake..."):
                    result = st.session_state.agent.load_data_from_adls()
                    st.success(result)
                    st.session_state.data_loaded = True
            else:
                st.error("Please enter OpenAI API key first")
    
    if st.session_state.data_loaded and st.session_state.agent:
        
        # Show data overview
        with st.expander("📊 Data Overview"):
            for sheet_name, summary in st.session_state.agent.data_summary["sheets"].items():
                st.write(f"**{sheet_name}**: {summary['rows']} rows, {summary['columns']} columns")
                st.write(f"Key manufacturing columns: {summary['key_manufacturing_columns'][:5]}")
        
        # Predefined insights for Surya's questions
        st.header("🎯 Key Manufacturing Insights")
        
        col1, col2, col3 = st.columns(3)
        
        if st.button("Generate Surya's Analysis", key="surya_analysis"):
            with st.spinner("Generating comprehensive insights..."):
                insights = st.session_state.agent.generate_predefined_insights()
                
                with col1:
                    st.metric("Total Lots", insights.get('total_lots', 'N/A'))
                    st.metric("High Yield Lots", insights.get('high_yield_count', 'N/A'))
                    
                with col2:
                    st.metric("Anomalies Detected", insights.get('anomalies_detected', 'N/A'))
                    
                with col3:
                    st.metric("Data Quality", "Good" if insights else "Processing...")
                
                # Feature importance chart
                if 'feature_importance' in insights:
                    st.subheader("🔍 Top Variables Impacting Yield")
                    fig = px.bar(insights['feature_importance'], 
                               x='importance', y='variable', 
                               orientation='h',
                               title="Feature Importance for Yield Prediction")
                    st.plotly_chart(fig, use_container_width=True)
        
        # Interactive query interface
        st.header("💬 Ask the Agent Anything")
        
        # Quick question buttons
        st.write("**Quick Questions:**")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("What causes defects?"):
                user_question = "What are the main variables that cause defects in manufacturing? Provide feature importance analysis."
        with col2:
            if st.button("Optimal operating ranges?"):
                user_question = "What are the optimal operating ranges for highest yield? Show me the parameter ranges of top performing lots."
        with col3:
            if st.button("Detect anomalies"):
                user_question = "Perform anomaly detection on the manufacturing data and show me unusual patterns."
        
        # Custom question input
        user_question = st.text_area("Or ask your own question:", 
                                   height=100,
                                   placeholder="E.g., 'Show me time series trends in yield over the last 6 months' or 'Which operators perform best at step 300?'")
        
        if st.button("🔍 Analyze", key="analyze_button"):
            if user_question:
                with st.spinner("Agent is thinking and analyzing..."):
                    
                    # Get AI response
                    ai_response = st.session_state.agent.ask_agent(user_question)
                    
                    # Display analysis plan
                    st.subheader("🧠 Analysis Plan")
                    st.write(f"**Analysis Type:** {ai_response.get('analysis_type', 'General Analysis')}")
                    st.write(f"**Relevant Sheets:** {ai_response.get('relevant_sheets', 'Auto-detected')}")
                    st.write(f"**Key Columns:** {ai_response.get('relevant_columns', 'Auto-detected')}")
                    
                    if 'analysis_plan' in ai_response:
                        st.write("**Steps:**")
                        for i, step in enumerate(ai_response['analysis_plan'], 1):
                            st.write(f"{i}. {step}")
                    
                    # Execute analysis
                    st.subheader("📊 Results")
                    results, fig = st.session_state.agent.execute_analysis(ai_response, user_question)
                    
                    # Display results
                    if 'error' in results:
                        st.error(results['error'])
                    else:
                        # Show any generated plots
                        if fig:
                            st.pyplot(fig)
                        
                        # Show data results
                        for key, value in results.items():
                            if isinstance(value, pd.DataFrame):
                                st.write(f"**{key}:**")
                                st.dataframe(value)
                            elif isinstance(value, (int, float)):
                                st.metric(key, value)
                            else:
                                st.write(f"**{key}:** {value}")
                    
                    # AI Interpretation
                    if 'interpretation' in ai_response:
                        st.subheader("🔍 Interpretation")
                        st.write(ai_response['interpretation'])
                    
                    if 'business_insights' in ai_response:
                        st.subheader("💼 Business Insights")
                        st.write(ai_response['business_insights'])
            
            else:
                st.warning("Please enter a question first")
    
    else:
        st.info("👆 Please configure your OpenAI API key and load data to start analyzing")
        
        # Example questions for preview
        st.subheader("Example Questions You Can Ask:")
        st.write("""
        - "What are the top 10 variables that impact manufacturing yield?"
        - "Show me anomalies in the production data from last month"
        - "Which operators have the best performance at step 300?"
        - "What are optimal parameter ranges for 95% yield?"
        - "Create a time series forecast for next month's production"
        - "Find correlations between cycle time and defect rates"
        - "Show me yield trends by product type over time"
        - "Which equipment (NODE) has the highest failure rate?"
        """)

if __name__ == "__main__":
    main()