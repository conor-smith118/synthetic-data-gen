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
        html.P("Generate synthetic data for your organization using AI", 
               className="text-center text-muted mb-5"),
        
        # Company Specifications Section
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H4("Company Specifications", className="mb-0")),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Company Name:", className="form-label fw-bold"),
                                dbc.Input(
                                    id="company-name",
                                    placeholder="Enter your company name (e.g., Acme Solutions Inc.)",
                                    value="Acme Solutions Inc.",
                                    className="mb-3"
                                )
                            ], width=6),
                            dbc.Col([
                                html.Label("Company Sector:", className="form-label fw-bold"),
                                dcc.Dropdown(
                                    id="company-sector",
                                    options=[
                                        {'label': 'Technology', 'value': 'technology'},
                                        {'label': 'Healthcare', 'value': 'healthcare'},
                                        {'label': 'Financial Services', 'value': 'financial_services'},
                                        {'label': 'Manufacturing', 'value': 'manufacturing'},
                                        {'label': 'Retail', 'value': 'retail'},
                                        {'label': 'Education', 'value': 'education'},
                                        {'label': 'Consulting', 'value': 'consulting'},
                                        {'label': 'Other', 'value': 'other'}
                                    ],
                                    value='technology',
                                    className="mb-3"
                                )
                            ], width=6)
                        ])
                    ])
                ], className="mb-4")
            ], width=12)
        ]),
        
        # Iterative Generation Section
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H4("Generate Synthetic Data", className="mb-0")),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Data Type:", className="form-label fw-bold"),
                                dcc.Dropdown(
                                    id='data-type-selector',
                                    options=[
                                        {'label': 'Generate PDF', 'value': 'pdf'},
                                        {'label': 'Generate Text', 'value': 'text'},
                                        {'label': 'Generate Tabular Data', 'value': 'tabular'}
                                    ],
                                    value='pdf',
                                    className="mb-3"
                                )
                            ], width=4),
                            dbc.Col([
                                html.Label("Description:", className="form-label fw-bold"),
                                dbc.Input(
                                    id='generation-description',
                                    placeholder="Describe what you want to generate...",
                                    className="mb-3"
                                )
                            ], width=6),
                            dbc.Col([
                                html.Label("Action:", className="form-label fw-bold"),
                                dbc.Button(
                                    "Generate",
                                    id="iterative-generate-button",
                                    color="success",
                                    size="lg",
                                    className="w-100"
                                )
                            ], width=2)
                        ])
                    ])
                ], className="mb-4")
            ], width=12)
        ]),
        
        # Generation History and Status
        dbc.Row([
            dbc.Col([
                html.Div(id="generation-history", className="mb-3"),
                html.Div(id="current-generation-status", className="mb-3"),
                dcc.Store(id="generation-store"),
                dcc.Store(id="progress-store"),
                dcc.Store(id="history-store", data=[]),
                dcc.Interval(
                    id="progress-interval",
                    interval=500,  # Update every 500ms
                    n_intervals=0,
                    disabled=True
                )
            ], width=12)
        ])
    ], fluid=True)

if __name__ == '__main__':
    app.run(debug=True)
