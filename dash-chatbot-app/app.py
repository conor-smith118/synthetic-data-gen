import os
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State, callback
from SyntheticDataGenerator import SyntheticDataGenerator
from model_serving_utils import is_endpoint_supported

# Ensure environment variable is set correctly
serving_endpoint = os.getenv('SERVING_ENDPOINT')
assert serving_endpoint, \
    ("Unable to determine serving endpoint to use for chatbot app. If developing locally, "
     "set the SERVING_ENDPOINT environment variable to the name of your serving endpoint. If "
     "deploying to a Databricks app, include a serving endpoint resource named "
     "'serving_endpoint' with CAN_QUERY permissions, as described in "
     "https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app#deploy-the-databricks-app")

# Check if the endpoint is supported
endpoint_supported = is_endpoint_supported(serving_endpoint)

# Initialize the Dash app with a clean theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

# Define the app layout based on endpoint support
if not endpoint_supported:
    app.layout = dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H2('Synthetic Data Generator', className='mb-3'),
                dbc.Alert([
                    html.H5("Endpoint Type Not Supported", className="alert-heading mb-3"),
                    html.P(f"The endpoint '{serving_endpoint}' is not compatible with this synthetic data generator.", 
                           className="mb-2"),
                    html.P("This app requires chat completions-compatible endpoints for generating synthetic data.", 
                           className="mb-3"),
                    html.Div([
                        html.P([
                            "Please ensure your endpoint supports chat completions. Visit the ",
                            html.A("Databricks documentation", 
                                   href="https://docs.databricks.com/aws/en/generative-ai/agent-framework/",
                                   target="_blank",
                                   className="alert-link"),
                            " for more information."
                        ], className="mb-0")
                    ])
                ], color="info", className="mt-4")
            ], width={'size': 8, 'offset': 2})
        ])
    ], fluid=True)
else:
    # Create the synthetic data generator component
    generator = SyntheticDataGenerator(app=app, endpoint_name=serving_endpoint)
    
    app.layout = dbc.Container([
        html.H1("Synthetic Data Generator", className="text-center mb-4"),
        html.P("Generate synthetic unstructured PDF documents using AI", 
               className="text-center text-muted mb-5"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Document Configuration", className="mb-4"),
                        
                        # Document Type Selection
                        html.Label("Document Type:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='document-type-dropdown',
                            options=[
                                {'label': 'Policy Guide', 'value': 'policy_guide'},
                                {'label': 'Customer Correspondence', 'value': 'customer_correspondence'},
                                {'label': 'Customer Profile', 'value': 'customer_profile'}
                            ],
                            value='policy_guide',
                            className="mb-3"
                        ),
                        
                        # Description Text Area
                        html.Label("Document Description:", className="form-label fw-bold"),
                        dbc.Textarea(
                            id='document-description',
                            placeholder="Describe the content and characteristics of the synthetic documents you want to generate...",
                            rows=4,
                            className="mb-3"
                        ),
                        
                        # Number of Documents
                        html.Label("Number of Documents:", className="form-label fw-bold"),
                        dcc.Slider(
                            id='document-count-slider',
                            min=1,
                            max=10,
                            step=1,
                            value=1,
                            marks={i: str(i) for i in range(1, 11)},
                            className="mb-4"
                        ),
                        
                        # Generate Button
                        dbc.Button(
                            "Generate Documents",
                            id="generate-button",
                            color="primary",
                            size="lg",
                            className="w-100 mb-3"
                        ),
                        
                        # Progress and Status
                        html.Div(id="generation-status", className="mb-3"),
                        dcc.Store(id="generation-store")
                    ])
                ])
            ], width={'size': 8, 'offset': 2})
        ])
    ], fluid=True)

if __name__ == '__main__':
    app.run(debug=True)
