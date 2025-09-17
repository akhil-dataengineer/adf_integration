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
        """Load all 7 sheets from ADLS with robust error handling"""
        try:
            conn_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};AccountKey={STORAGE_KEY};EndpointSuffix=core.windows.net"
            service_client = BlobServiceClient.from_connection_string(conn_str)
            container_client = service_client.get_container_client(CONTAINER)
            
            blob_client = container_client.get_blob_client("Envista clean dataset.xlsx")
            stream = io.BytesIO(blob_client.download_blob().readall())
            
            # Load all sheets with proper error handling
            all_sheets = pd.read_excel(stream, sheet_name=None, engine='openpyxl')
            self.data = all_sheets
            
            # Clean and prepare data
            self.data = self._clean_data(all_sheets)
            
            # Create comprehensive data summary for the AI
            self.data_summary = self._create_data_summary()
            
            return f"Successfully loaded {len(all_sheets)} sheets: {list(all_sheets.keys())}"
            
        except Exception as e:
            return f"Error loading data: {str(e)}"
    
    def _clean_data(self, data_dict):
        """Clean and prepare data for analysis"""
        cleaned_data = {}
        
        for sheet_name, df in data_dict.items():
            try:
                # Basic cleaning
                df_clean = df.copy()
                
                # Remove completely empty rows and columns
                df_clean = df_clean.dropna(how='all').dropna(axis=1, how='all')
                
                # Convert numeric columns properly
                for col in df_clean.columns:
                    if df_clean[col].dtype == 'object':
                        # Try to convert to numeric if possible
                        numeric_series = pd.to_numeric(df_clean[col], errors='coerce')
                        if not numeric_series.isna().all():
                            df_clean[col] = numeric_series
                
                cleaned_data[sheet_name] = df_clean
                
            except Exception as e:
                st.warning(f"Error cleaning sheet {sheet_name}: {str(e)}")
                cleaned_data[sheet_name] = df
        
        return cleaned_data
    
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
            try:
                summary["sheets"][sheet_name] = {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": list(df.columns),
                    "data_types": {str(k): str(v) for k, v in df.dtypes.to_dict().items()},
                    "numeric_columns": list(df.select_dtypes(include=[np.number]).columns),
                    "categorical_columns": list(df.select_dtypes(include=['object']).columns),
                    "missing_percentage": {str(k): float(v/len(df)*100) for k, v in df.isnull().sum().to_dict().items()}
                }
                
                # Identify potential key columns for manufacturing
                manufacturing_patterns = ['BEGQTY', 'ENDQTY', 'OPER', 'NODE', 'YIELD', 'DEFECT', 'BATCH', 'LOT', 'QTY']
                key_columns = [col for col in df.columns 
                              if any(pattern in str(col).upper() for pattern in manufacturing_patterns)]
                summary["sheets"][sheet_name]["key_manufacturing_columns"] = key_columns
                
            except Exception as e:
                st.warning(f"Error summarizing sheet {sheet_name}: {str(e)}")
        
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
        4. Generate Python code to execute the analysis with proper error handling
        5. Interpret results and provide actionable insights
        
        IMPORTANT: Generate robust Python code that handles missing data, data type issues, and edge cases.
        
        Return your response as JSON with these keys:
        - "analysis_type": type of analysis needed
        - "relevant_sheets": which sheets to use
        - "relevant_columns": key columns to analyze
        - "analysis_plan": step-by-step plan as list
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
                "python_code": "# Error occurred in AI processing",
                "analysis_plan": ["Error in AI processing"],
                "interpretation": "AI processing failed",
                "business_insights": "Please try again or contact support"
            }
    
    def execute_analysis(self, ai_response, user_question):
        """Execute the analysis code generated by AI with robust error handling"""
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
            
            # Execute the AI-generated code with error handling
            try:
                exec(ai_response.get('python_code', ''), exec_globals)
            except Exception as code_error:
                exec_globals['results'] = {"code_execution_error": str(code_error)}
            
            return exec_globals.get('results', {}), exec_globals.get('fig', None)
            
        except Exception as e:
            return {"execution_error": f"Execution error: {str(e)}"}, None
    
    def generate_predefined_insights(self):
        """Generate predefined insights for Surya's questions - optimized for your specific data"""
        insights = {}
        
        try:
            # Target your main manufacturing sheet specifically
            main_sheet = None
            main_sheet_name = None
            
            # Priority order for your sheets
            preferred_sheets = ['2024 to Mar 2025', 'Sheet1', 'Operator Rating']
            
            for preferred in preferred_sheets:
                if preferred in self.data:
                    main_sheet = self.data[preferred]
                    main_sheet_name = preferred
                    break
            
            if main_sheet is None and self.data:
                # Fallback to largest sheet
                main_sheet_name = max(self.data.keys(), key=lambda x: len(self.data[x]))
                main_sheet = self.data[main_sheet_name]
            
            insights['main_sheet_used'] = main_sheet_name
            insights['total_rows'] = len(main_sheet) if main_sheet is not None else 0
            insights['total_columns'] = len(main_sheet.columns) if main_sheet is not None else 0
            
            if main_sheet is not None and len(main_sheet) > 100:  # Only analyze if substantial data
                
                # 1. Manufacturing-specific yield analysis
                yield_calculated = False
                
                # Look for your specific yield patterns
                if 'YIELD_QTY' in main_sheet.columns:
                    yield_data = pd.to_numeric(main_sheet['YIELD_QTY'], errors='coerce').dropna()
                    if len(yield_data) > 0:
                        insights['yield_stats'] = {
                            'mean': float(yield_data.mean()),
                            'median': float(yield_data.median()),
                            'std': float(yield_data.std()),
                            'min': float(yield_data.min()),
                            'max': float(yield_data.max())
                        }
                        yield_calculated = True
                
                # Try ENDQTY/BEGQTY calculation for steps
                if not yield_calculated:
                    end_cols = [col for col in main_sheet.columns if 'ENDQTY' in str(col)]
                    beg_cols = [col for col in main_sheet.columns if 'BEGQTY' in str(col)]
                    
                    if 'ENDQTY200' in main_sheet.columns and 'BEGQTY200' in main_sheet.columns:
                        try:
                            end_qty = pd.to_numeric(main_sheet['ENDQTY200'], errors='coerce')
                            beg_qty = pd.to_numeric(main_sheet['BEGQTY200'], errors='coerce')
                            
                            # Calculate yield with proper handling of zeros and invalid values
                            valid_mask = (beg_qty > 0) & (end_qty >= 0) & (~pd.isna(beg_qty)) & (~pd.isna(end_qty))
                            if valid_mask.sum() > 10:
                                calculated_yield = (end_qty[valid_mask] / beg_qty[valid_mask]).clip(0, 2)  # Cap at 200%
                                
                                insights['step_200_yield'] = {
                                    'mean': float(calculated_yield.mean()),
                                    'median': float(calculated_yield.median()),
                                    'std': float(calculated_yield.std()),
                                    'valid_lots': int(valid_mask.sum())
                                }
                                yield_calculated = True
                        except Exception:
                            pass
                
                # 2. Operator analysis if available
                if 'Operator Rating' in self.data:
                    op_sheet = self.data['Operator Rating']
                    if 'OPERATOR_ID' in op_sheet.columns or 'Operator Name' in op_sheet.columns:
                        insights['operators_available'] = len(op_sheet)
                        insights['unique_operators'] = op_sheet.nunique().max()
                
                # 3. Equipment analysis (NODE columns)
                node_cols = [col for col in main_sheet.columns if 'NODE' in str(col)]
                if node_cols:
                    insights['equipment_types'] = len(node_cols)
                    # Count unique equipment
                    unique_equipment = set()
                    for col in node_cols:
                        unique_vals = main_sheet[col].dropna().unique()
                        unique_equipment.update(unique_vals)
                    insights['unique_equipment_count'] = len(unique_equipment)
                
                # 4. Process step analysis
                process_steps = []
                for col in main_sheet.columns:
                    if any(pattern in str(col) for pattern in ['OPER', 'BEGQTY', 'ENDQTY']):
                        # Extract step number
                        for char_seq in str(col):
                            if char_seq.isdigit():
                                step_num = ''.join([c for c in str(col) if c.isdigit()])
                                if step_num and len(step_num) >= 3:
                                    process_steps.append(step_num)
                                break
                
                insights['process_steps_identified'] = list(set(process_steps))
                
                # 5. Safe anomaly detection on key numeric columns
                key_numeric_cols = []
                for col in main_sheet.columns:
                    if any(pattern in str(col).upper() for pattern in ['QTY', 'OPER', 'YIELD']) and main_sheet[col].dtype in ['int64', 'float64']:
                        key_numeric_cols.append(col)
                
                if len(key_numeric_cols) >= 3:
                    try:
                        # Use only columns with reasonable variance
                        anomaly_data = main_sheet[key_numeric_cols].fillna(0)
                        anomaly_data = anomaly_data.select_dtypes(include=[np.number])
                        
                        # Remove zero-variance columns
                        variance_mask = anomaly_data.var() > 0
                        anomaly_data = anomaly_data.loc[:, variance_mask]
                        
                        if len(anomaly_data.columns) >= 2 and len(anomaly_data) >= 50:
                            # Use smaller contamination for manufacturing data
                            iso_forest = IsolationForest(contamination=0.05, random_state=42, n_estimators=100)
                            anomalies = iso_forest.fit_predict(anomaly_data)
                            insights['anomalies_detected'] = int((anomalies == -1).sum())
                            insights['anomaly_percentage'] = float((anomalies == -1).sum() / len(anomalies) * 100)
                    
                    except Exception as e:
                        insights['anomaly_note'] = f"Anomaly detection skipped: {str(e)}"
                
                # 6. Manufacturing-specific feature importance
                if yield_calculated and 'step_200_yield' in insights:
                    try:
                        # Find features that correlate with yield
                        numeric_features = main_sheet.select_dtypes(include=[np.number]).columns
                        feature_data = main_sheet[numeric_features].fillna(0)
                        
                        # Recreate yield for correlation
                        if 'ENDQTY200' in feature_data.columns and 'BEGQTY200' in feature_data.columns:
                            end_qty = feature_data['ENDQTY200']
                            beg_qty = feature_data['BEGQTY200']
                            valid_mask = (beg_qty > 0) & (end_qty >= 0)
                            
                            if valid_mask.sum() > 20:
                                yield_series = pd.Series(index=feature_data.index, dtype=float)
                                yield_series.loc[valid_mask] = (end_qty[valid_mask] / beg_qty[valid_mask]).clip(0, 2)
                                
                                # Calculate correlations
                                correlations = feature_data.corrwith(yield_series).abs().sort_values(ascending=False)
                                correlations = correlations.dropna().head(10)
                                
                                if len(correlations) > 0:
                                    feature_importance_df = pd.DataFrame({
                                        'variable': correlations.index,
                                        'correlation': correlations.values
                                    })
                                    insights['top_yield_factors'] = feature_importance_df
                    
                    except Exception:
                        insights['feature_analysis'] = 'Correlation analysis incomplete'
                
                # 7. Data quality metrics
                insights['data_quality'] = {
                    'total_missing_values': int(main_sheet.isnull().sum().sum()),
                    'missing_percentage': float(main_sheet.isnull().sum().sum() / main_sheet.size * 100),
                    'duplicate_rows': int(main_sheet.duplicated().sum()),
                    'numeric_columns': len(main_sheet.select_dtypes(include=[np.number]).columns),
                    'categorical_columns': len(main_sheet.select_dtypes(include=['object']).columns)
                }
            
        except Exception as e:
            insights['generation_error'] = f"Error in insights generation: {str(e)}"
        
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
                if summary['key_manufacturing_columns']:
                    st.write(f"Key manufacturing columns: {summary['key_manufacturing_columns'][:10]}")
        
        # Predefined insights for Surya's questions
        st.header("🎯 Key Manufacturing Insights")
        
        if st.button("Generate Surya's Analysis", key="surya_analysis"):
            with st.spinner("Generating comprehensive insights..."):
                insights = st.session_state.agent.generate_predefined_insights()
                
                # Display metrics
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Total Rows", insights.get('total_rows', 'N/A'))
                    st.metric("Total Columns", insights.get('total_columns', 'N/A'))
                    
                with col2:
                    st.metric("Numeric Columns", insights.get('numeric_columns_count', 'N/A'))
                    if 'anomalies_detected' in insights:
                        st.metric("Anomalies Detected", insights['anomalies_detected'])
                    
                with col3:
                    if 'data_quality' in insights:
                        missing_pct = insights['data_quality']['missing_data_percentage']
                        st.metric("Missing Data %", f"{missing_pct:.1f}%")
                
                # Display additional insights
                if 'calculated_yield_stats' in insights:
                    st.subheader("📈 Yield Analysis")
                    yield_stats = insights['calculated_yield_stats']
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Average Yield", f"{yield_stats['mean']:.3f}")
                    with col2:
                        st.metric("Median Yield", f"{yield_stats['median']:.3f}")
                    with col3:
                        st.metric("Yield Std Dev", f"{yield_stats['std']:.3f}")
                
                # Feature importance chart
                if 'feature_importance' in insights and not insights['feature_importance'].empty:
                    st.subheader("🔍 Top Variables by Correlation")
                    fig = px.bar(insights['feature_importance'], 
                               x='importance', y='variable', 
                               orientation='h',
                               title="Feature Importance (Correlation Analysis)")
                    st.plotly_chart(fig, use_container_width=True)
                
                # Display any errors or warnings
                if 'error' in insights:
                    st.error(f"Analysis Error: {insights['error']}")
                if 'ml_analysis_error' in insights:
                    st.warning(f"ML Analysis Issue: {insights['ml_analysis_error']}")
        
        # Interactive query interface
        st.header("💬 Ask the Agent Anything")
        
        # Quick question buttons
        st.write("**Quick Questions:**")
        col1, col2, col3 = st.columns(3)
        
        user_question = ""
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
        if not user_question:
            user_question = st.text_area("Or ask your own question:", 
                                       height=100,
                                       placeholder="E.g., 'Show me time series trends in yield over the last 6 months' or 'Which operators perform best at step 300?'")
        
        if st.button("🔍 Analyze", key="analyze_button") and user_question:
            with st.spinner("Agent is thinking and analyzing..."):
                
                # Get AI response
                ai_response = st.session_state.agent.ask_agent(user_question)
                
                # Display analysis plan
                st.subheader("🧠 Analysis Plan")
                st.write(f"**Analysis Type:** {ai_response.get('analysis_type', 'General Analysis')}")
                st.write(f"**Relevant Sheets:** {ai_response.get('relevant_sheets', 'Auto-detected')}")
                st.write(f"**Key Columns:** {ai_response.get('relevant_columns', 'Auto-detected')}")
                
                if 'analysis_plan' in ai_response and isinstance(ai_response['analysis_plan'], list):
                    st.write("**Steps:**")
                    for i, step in enumerate(ai_response['analysis_plan'], 1):
                        st.write(f"{i}. {step}")
                
                # Execute analysis
                st.subheader("📊 Results")
                results, fig = st.session_state.agent.execute_analysis(ai_response, user_question)
                
                # Display results
                if any(key in results for key in ['error', 'execution_error', 'code_execution_error']):
                    for error_key in ['error', 'execution_error', 'code_execution_error']:
                        if error_key in results:
                            st.error(f"Analysis Error: {results[error_key]}")
                else:
                    # Show any generated plots
                    if fig:
                        st.pyplot(fig)
                    
                    # Show data results
                    for key, value in results.items():
                        if isinstance(value, pd.DataFrame) and not value.empty:
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