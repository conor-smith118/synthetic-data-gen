import dash
from dash import html, Input, Output, State, dcc, callback_context
import dash_bootstrap_components as dbc
from model_serving_utils import query_endpoint
import json
import os
import time
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from databricks.sdk import WorkspaceClient
import threading
import io
from docx import Document
from dbldatagen import DataGenerator, fakerText

class SyntheticDataGenerator:
    def __init__(self, app, endpoint_name):
        self.app = app
        self.endpoint_name = endpoint_name
        self.volume_path = "/Volumes/conor_smith/synthetic_data_app/synthetic_data_volume"
        
        # Progress tracking
        self.generation_state = {
            'active': False,
            'current_doc': 0,
            'total_docs': 0,
            'current_step': '',
            'completed_files': [],
            'error': None,
            'start_time': None
        }
        
        self._create_callbacks()
        self._add_custom_css()

    def _create_callbacks(self):
        # Enable/disable add operation button based on data type selection
        @self.app.callback(
            Output('add-operation-button', 'disabled'),
            Input('data-type-selector', 'value'),
            prevent_initial_call=True
        )
        def enable_add_operation(data_type):
            return data_type is None
        
        # Add operation callback
        @self.app.callback(
            Output('operations-store', 'data'),
            Output('data-type-selector', 'value'),
            Input('add-operation-button', 'n_clicks'),
            State('data-type-selector', 'value'),
            State('operations-store', 'data'),
            prevent_initial_call=True
        )
        def add_operation(n_clicks, data_type, operations):
            if not n_clicks or not data_type:
                return dash.no_update, dash.no_update
            
            operations = operations or []
            
            # Create new operation with unique ID
            operation_id = f"op_{len(operations)}_{int(time.time())}"
            new_operation = {
                'id': operation_id,
                'type': data_type,
                'configured': False,
                'config': {}
            }
            
            operations.append(new_operation)
            
            # Clear the selector
            return operations, None
        
        # Display operations callback
        @self.app.callback(
            Output('operations-container', 'children'),
            Output('generate-all-button', 'disabled'),
            Input('operations-store', 'data'),
            prevent_initial_call=True
        )
        def display_operations(operations):
            if not operations:
                return [], True
            
            operation_cards = []
            configured_count = 0
            
            for op in operations:
                card = self._create_operation_card(op)
                operation_cards.append(card)
                if op.get('configured', False):
                    configured_count += 1
            
            # Enable generate button if at least one operation is configured
            generate_disabled = configured_count == 0
            
            return operation_cards, generate_disabled

        # Start batch generation callback
        @self.app.callback(
            Output('progress-interval', 'disabled'),
            Output('progress-store', 'data'),
            Output('generate-all-button', 'disabled', allow_duplicate=True),
            Input('generate-all-button', 'n_clicks'),
            State('operations-store', 'data'),
            State('company-name', 'value'),
            State('company-sector', 'value'),
            prevent_initial_call=True
        )
        def start_batch_generation(n_clicks, operations, company_name, company_sector):
            if not n_clicks or not operations:
                return dash.no_update, dash.no_update, dash.no_update
            
            # Filter only configured operations
            configured_ops = [op for op in operations if op.get('configured', False)]
            if not configured_ops:
                return dash.no_update, dash.no_update, dash.no_update
            
            # Reset and start generation state
            self.generation_state = {
                'active': True,
                'current_operation': 0,
                'total_operations': len(configured_ops),
                'current_step': 'Initializing batch...',
                'completed_items': [],
                'error': None,
                'start_time': time.time(),
                'operations': configured_ops,
                'company_name': company_name or 'Acme Solutions Inc.',
                'company_sector': company_sector or 'technology'
            }
            
            # Start generation in background thread
            threading.Thread(
                target=self._generate_batch_background, 
                args=(configured_ops, company_name, company_sector),
                daemon=True
            ).start()
            
            # Enable interval and disable button
            return False, {'status': 'started'}, True
        
        # Progress update callback
        @self.app.callback(
            Output('current-generation-status', 'children'),
            Output('progress-interval', 'disabled', allow_duplicate=True),
            Output('generate-all-button', 'disabled', allow_duplicate=True),
            Output('generation-store', 'data'),
            Input('progress-interval', 'n_intervals'),
            prevent_initial_call=True
        )
        def update_progress(n_intervals):
            if not self.generation_state.get('active', False) and self.generation_state.get('current_operation', 0) == 0:
                return dash.no_update, dash.no_update, dash.no_update, dash.no_update
            
            # Calculate progress
            progress_percent = 0
            if self.generation_state.get('total_operations', 0) > 0:
                progress_percent = (self.generation_state.get('current_operation', 0) / self.generation_state.get('total_operations', 1)) * 100
            
            # Check if generation is complete
            if not self.generation_state.get('active', False):
                if self.generation_state.get('error'):
                    # Error state
                    error_message = dbc.Alert([
                        html.H5("❌ Batch Generation Failed", className="mb-2"),
                        html.P(f"Error: {self.generation_state['error']}")
                    ], color="danger")
                    return error_message, True, False, {'status': 'error', 'error': self.generation_state['error']}
                else:
                    # Success state - batch completed
                    elapsed_time = time.time() - self.generation_state['start_time']
                    total_items = len(self.generation_state.get('completed_items', []))
                    
                    success_message = dbc.Alert([
                        html.H5("✅ Batch Generation Complete!", className="mb-2"),
                        html.P(f"Successfully generated {total_items} item(s) from {self.generation_state.get('total_operations', 0)} operation(s) in {elapsed_time:.1f} seconds"),
                        html.P("You can add more operations above or generate the current batch again!", className="mb-0 text-muted")
                    ], color="success")
                    
                    return success_message, True, False, {'status': 'complete', 'items': self.generation_state.get('completed_items', [])}
            
            # Active generation state
            current_op = self.generation_state.get('current_operation', 0)
            total_ops = self.generation_state.get('total_operations', 1)
            
            status_message = dbc.Alert([
                html.Div([
                    html.Div([
                        html.I(className="fas fa-spinner fa-spin me-2"),
                        html.H5(f"🔄 Processing Batch Operations...", className="d-inline mb-0")
                    ], className="d-flex align-items-center mb-3"),
                    
                    html.P(f"Operation {current_op} of {total_ops}", className="mb-2"),
                    html.P(f"Current step: {self.generation_state.get('current_step', 'Processing...')}", className="mb-3 text-muted"),
                    
                    dbc.Progress(
                        value=progress_percent,
                        striped=True,
                        animated=True,
                        className="mb-2"
                    ),
                    html.P(f"{progress_percent:.0f}% Complete", className="mb-0 text-center small")
                ])
            ], color="info")
            
            return status_message, False, True, {'status': 'generating', 'progress': progress_percent}
        
        # History update callback
        @self.app.callback(
            Output('generation-history', 'children'),
            Output('history-store', 'data'),
            Input('generation-store', 'data'),
            State('history-store', 'data'),
            prevent_initial_call=True
        )
        def update_history(generation_data, history_data):
            if not generation_data or generation_data.get('status') != 'complete':
                return dash.no_update, dash.no_update
            
            # Add new generation to history
            history_data = history_data or []
            
            new_entry = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'data_type': self.generation_state.get('data_type', 'unknown'),
                'description': self.generation_state.get('description', 'No description'),
                'company_name': self.generation_state.get('company_name', 'Unknown Company'),
                'company_sector': self.generation_state.get('company_sector', 'unknown'),
                'items': self.generation_state.get('completed_items', [])
            }
            
            history_data.append(new_entry)
            
            # Build history display
            history_cards = []
            for i, entry in enumerate(reversed(history_data[-10:])):  # Show last 10 entries, most recent first
                history_cards.append(
                    dbc.Card([
                        dbc.CardBody([
                            html.Div([
                                html.H6(f"{self._format_data_type(entry['data_type'])}", className="card-title mb-1"),
                                html.P(f"Company: {entry['company_name']} ({entry['company_sector']})", className="mb-1 small text-muted"),
                                html.P(f"Description: {entry['description']}", className="mb-1"),
                                html.P(f"Generated: {entry['timestamp']}", className="mb-1 small text-muted"),
                                self._format_items_display(entry['items'], entry['data_type'])
                            ])
                        ])
                    ], className="mb-2")
                )
            
            if history_cards:
                history_display = [
                    html.H4("Generation History", className="mb-3"),
                    html.Div(history_cards)
                ]
            else:
                history_display = []
            
            return history_display, history_data
        
        # Dynamic callback for operation configuration updates
        @self.app.callback(
            Output('operations-store', 'data', allow_duplicate=True),
            [Input({'type': 'pdf-description', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'pdf-doc-type', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'pdf-count', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'text-description', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'text-doc-type', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'text-format', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'text-count', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'tabular-name', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'tabular-rows', 'index': dash.dependencies.ALL}, 'value')],
            State('operations-store', 'data'),
            prevent_initial_call=True
        )
        def update_operation_configs(*args):
            operations = args[-1]  # Last argument is the operations state
            if not operations:
                return dash.no_update
            
            ctx = callback_context
            if not ctx.triggered:
                return dash.no_update
            
            # Parse the triggered input
            triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
            if triggered_id == '':
                return dash.no_update
            
            try:
                import json
                triggered_comp = json.loads(triggered_id)
                op_id = triggered_comp['index']
                comp_type = triggered_comp['type']
                new_value = ctx.triggered[0]['value']
                
                # Find and update the operation
                for op in operations:
                    if op['id'] == op_id:
                        if 'config' not in op:
                            op['config'] = {}
                        
                        # Update the specific config value
                        if comp_type == 'pdf-description':
                            op['config']['description'] = new_value
                        elif comp_type == 'pdf-doc-type':
                            op['config']['doc_type'] = new_value
                        elif comp_type == 'pdf-count':
                            op['config']['count'] = new_value
                        elif comp_type == 'text-description':
                            op['config']['description'] = new_value
                        elif comp_type == 'text-doc-type':
                            op['config']['doc_type'] = new_value
                        elif comp_type == 'text-format':
                            op['config']['file_format'] = new_value
                        elif comp_type == 'text-count':
                            op['config']['count'] = new_value
                        elif comp_type == 'tabular-name':
                            op['config']['table_name'] = new_value
                        elif comp_type == 'tabular-rows':
                            op['config']['row_count'] = new_value
                        
                        # Mark as configured based on operation type
                        if op['type'] == 'tabular':
                            # For tabular operations, require table name and at least one column
                            table_name = op['config'].get('table_name', '')
                            columns = op['config'].get('columns', [])
                            op['configured'] = bool(table_name and table_name.strip() and len(columns) > 0)
                        else:
                            # For other operations, require description
                            description = op['config'].get('description', '')
                            op['configured'] = bool(description and description.strip())
                        break
                
                return operations
                
            except Exception as e:
                print(f"Error updating operation config: {str(e)}")
                return dash.no_update
        
        # Remove operation callback
        @self.app.callback(
            Output('operations-store', 'data', allow_duplicate=True),
            Input({'type': 'remove-op', 'index': dash.dependencies.ALL}, 'n_clicks'),
            State('operations-store', 'data'),
            prevent_initial_call=True
        )
        def remove_operation(n_clicks_list, operations):
            if not operations or not any(n_clicks_list):
                return dash.no_update
            
            ctx = callback_context
            if not ctx.triggered:
                return dash.no_update
            
            # Find which remove button was clicked
            triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
            if triggered_id == '':
                return dash.no_update
            
            try:
                import json
                triggered_comp = json.loads(triggered_id)
                op_id_to_remove = triggered_comp['index']
                
                # Remove the operation with matching ID
                operations = [op for op in operations if op['id'] != op_id_to_remove]
                
                return operations
                
            except Exception as e:
                print(f"Error removing operation: {str(e)}")
                return dash.no_update
        
        # Add column callback
        @self.app.callback(
            Output('operations-store', 'data', allow_duplicate=True),
            Input({'type': 'add-column', 'index': dash.dependencies.ALL}, 'n_clicks'),
            State('operations-store', 'data'),
            prevent_initial_call=True
        )
        def add_column(n_clicks_list, operations):
            if not operations or not any(n_clicks_list):
                return dash.no_update
            
            ctx = callback_context
            if not ctx.triggered:
                return dash.no_update
            
            try:
                import json
                triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
                triggered_comp = json.loads(triggered_id)
                op_id = triggered_comp['index']
                
                # Find the operation and add a new column
                for op in operations:
                    if op['id'] == op_id and op['type'] == 'tabular':
                        if 'columns' not in op['config']:
                            op['config']['columns'] = []
                        
                        # Create new column with unique ID
                        new_column = {
                            'id': f"col_{len(op['config']['columns'])}_{int(time.time())}",
                            'name': f"column_{len(op['config']['columns']) + 1}",
                            'data_type': 'Integer',
                            'min_value': 1,
                            'max_value': 100
                        }
                        op['config']['columns'].append(new_column)
                        
                        # Update configured status
                        table_name = op['config'].get('table_name', '')
                        op['configured'] = bool(table_name and table_name.strip() and len(op['config']['columns']) > 0)
                        break
                
                return operations
                
            except Exception as e:
                print(f"Error adding column: {str(e)}")
                return dash.no_update
        
        # Remove column callback
        @self.app.callback(
            Output('operations-store', 'data', allow_duplicate=True),
            Input({'type': 'remove-column', 'op': dash.dependencies.ALL, 'col': dash.dependencies.ALL}, 'n_clicks'),
            State('operations-store', 'data'),
            prevent_initial_call=True
        )
        def remove_column(n_clicks_list, operations):
            if not operations or not any(n_clicks_list):
                return dash.no_update
            
            ctx = callback_context
            if not ctx.triggered:
                return dash.no_update
            
            try:
                import json
                triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
                triggered_comp = json.loads(triggered_id)
                op_id = triggered_comp['op']
                col_id = triggered_comp['col']
                
                # Find the operation and remove the column
                for op in operations:
                    if op['id'] == op_id and op['type'] == 'tabular':
                        if 'columns' in op['config']:
                            op['config']['columns'] = [col for col in op['config']['columns'] if col['id'] != col_id]
                            
                            # Update configured status
                            table_name = op['config'].get('table_name', '')
                            op['configured'] = bool(table_name and table_name.strip() and len(op['config']['columns']) > 0)
                        break
                
                return operations
                
            except Exception as e:
                print(f"Error removing column: {str(e)}")
                return dash.no_update
        
        # Update columns container callback
        @self.app.callback(
            Output({'type': 'columns-container', 'index': dash.dependencies.MATCH}, 'children'),
            Input('operations-store', 'data'),
            State({'type': 'columns-container', 'index': dash.dependencies.MATCH}, 'id'),
            prevent_initial_call=True
        )
        def update_columns_container(operations, container_id):
            if not operations:
                return []
            
            op_id = container_id['index']
            
            # Find the matching operation
            for op in operations:
                if op['id'] == op_id and op['type'] == 'tabular':
                    columns = op['config'].get('columns', [])
                    return self._create_column_cards(columns, op_id)
            
            return []

    def _create_operation_card(self, operation):
        """Create a card for configuring an operation."""
        op_id = operation['id']
        op_type = operation['type']
        configured = operation.get('configured', False)
        config = operation.get('config', {})
        
        # Create operation-specific configuration inputs
        if op_type == 'pdf':
            config_inputs = [
                html.Label("Document Type:", className="form-label fw-bold"),
                dcc.Dropdown(
                    id={'type': 'pdf-doc-type', 'index': op_id},
                    options=[
                        {'label': 'Policy Guide', 'value': 'policy_guide'},
                        {'label': 'Customer Correspondence', 'value': 'customer_correspondence'},
                        {'label': 'Customer Profile', 'value': 'customer_profile'}
                    ],
                    value=config.get('doc_type', 'policy_guide'),
                    className="mb-3"
                ),
                html.Label("Description:", className="form-label fw-bold"),
                dbc.Textarea(
                    id={'type': 'pdf-description', 'index': op_id},
                    placeholder="Describe the PDF document you want to generate...",
                    value=config.get('description', ''),
                    rows=3,
                    className="mb-3"
                ),
                html.Label("Number of Documents:", className="form-label fw-bold"),
                dcc.Slider(
                    id={'type': 'pdf-count', 'index': op_id},
                    min=1,
                    max=10,
                    step=1,
                    value=config.get('count', 1),
                    marks={i: str(i) for i in range(1, 11)},
                    className="mb-3"
                )
            ]
        elif op_type == 'text':
            config_inputs = [
                html.Label("Document Type:", className="form-label fw-bold"),
                dcc.Dropdown(
                    id={'type': 'text-doc-type', 'index': op_id},
                    options=[
                        {'label': 'Policy Guide', 'value': 'policy_guide'},
                        {'label': 'Customer Correspondence', 'value': 'customer_correspondence'},
                        {'label': 'Customer Profile', 'value': 'customer_profile'}
                    ],
                    value=config.get('doc_type', 'policy_guide'),
                    className="mb-3"
                ),
                html.Label("File Format:", className="form-label fw-bold"),
                dcc.Dropdown(
                    id={'type': 'text-format', 'index': op_id},
                    options=[
                        {'label': 'Text File (.txt)', 'value': 'txt'},
                        {'label': 'Word Document (.docx)', 'value': 'docx'}
                    ],
                    value=config.get('file_format', 'txt'),
                    className="mb-3"
                ),
                html.Label("Description:", className="form-label fw-bold"),
                dbc.Textarea(
                    id={'type': 'text-description', 'index': op_id},
                    placeholder="Describe the text document you want to generate...",
                    value=config.get('description', ''),
                    rows=3,
                    className="mb-3"
                ),
                html.Label("Number of Documents:", className="form-label fw-bold"),
                dcc.Slider(
                    id={'type': 'text-count', 'index': op_id},
                    min=1,
                    max=10,
                    step=1,
                    value=config.get('count', 1),
                    marks={i: str(i) for i in range(1, 11)},
                    className="mb-3"
                )
            ]
        elif op_type == 'tabular':
            config_inputs = [
                html.Label("Table Name:", className="form-label fw-bold"),
                dbc.Input(
                    id={'type': 'tabular-name', 'index': op_id},
                    placeholder="Enter table name...",
                    value=config.get('table_name', ''),
                    className="mb-3"
                ),
                html.Label("Number of Rows:", className="form-label fw-bold"),
                dcc.Slider(
                    id={'type': 'tabular-rows', 'index': op_id},
                    min=1,
                    max=50000,
                    step=1,
                    value=config.get('row_count', 1000),
                    marks={
                        1: '1',
                        100: '100',
                        1000: '1K',
                        5000: '5K',
                        10000: '10K',
                        25000: '25K',
                        50000: '50K'
                    },
                    tooltip={"placement": "bottom", "always_visible": True},
                    className="mb-3"
                ),
                html.Div([
                    html.Div([
                        html.Label("Columns:", className="form-label fw-bold"),
                        dbc.Button(
                            "Add Column",
                            id={'type': 'add-column', 'index': op_id},
                            color="primary",
                            size="sm",
                            className="float-end"
                        )
                    ], className="d-flex justify-content-between align-items-center mb-3"),
                    html.Div(
                        id={'type': 'columns-container', 'index': op_id},
                        children=self._create_column_cards(config.get('columns', []), op_id),
                        className="mb-3"
                    )
                ])
            ]
        else:
            config_inputs = [html.P("Unknown operation type")]
        
        # Status indicator
        if configured:
            status_color = "success"
            status_icon = "✅"
            status_text = "Configured"
        else:
            status_color = "warning"
            status_icon = "⚠️"
            status_text = "Needs Configuration"
        
        return dbc.Card([
            dbc.CardHeader([
                html.Div([
                    html.H5(f"{self._format_data_type(op_type)}", className="mb-0"),
                    html.Div([
                        dbc.Badge(f"{status_icon} {status_text}", color=status_color, className="me-2"),
                        dbc.Button("Remove", id={'type': 'remove-op', 'index': op_id}, color="danger", size="sm")
                    ])
                ], className="d-flex justify-content-between align-items-center")
            ]),
            dbc.CardBody(config_inputs)
        ], className="mb-3")

    def _create_column_cards(self, columns, op_id):
        """Create cards for column configuration within a tabular operation."""
        if not columns:
            return []
        
        column_cards = []
        for i, col in enumerate(columns):
            col_id = col.get('id', f"col_{i}")
            col_name = col.get('name', '')
            col_type = col.get('data_type', 'Integer')
            
            # Create type-specific configuration inputs
            type_inputs = []
            if col_type == 'Integer':
                type_inputs = [
                    dbc.Row([
                        dbc.Col([
                            html.Label("Min Value:", className="form-label"),
                            dbc.Input(
                                id={'type': 'col-min', 'op': op_id, 'col': col_id},
                                type="number",
                                value=col.get('min_value', 1),
                                size="sm"
                            )
                        ], width=6),
                        dbc.Col([
                            html.Label("Max Value:", className="form-label"),
                            dbc.Input(
                                id={'type': 'col-max', 'op': op_id, 'col': col_id},
                                type="number",
                                value=col.get('max_value', 100),
                                size="sm"
                            )
                        ], width=6)
                    ])
                ]
            
            card = dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Column Name:", className="form-label"),
                            dbc.Input(
                                id={'type': 'col-name', 'op': op_id, 'col': col_id},
                                value=col_name,
                                size="sm",
                                placeholder="Enter column name..."
                            )
                        ], width=4),
                        dbc.Col([
                            html.Label("Data Type:", className="form-label"),
                            dcc.Dropdown(
                                id={'type': 'col-type', 'op': op_id, 'col': col_id},
                                options=[
                                    {'label': 'Integer', 'value': 'Integer'},
                                    {'label': 'First Name', 'value': 'First Name'},
                                    {'label': 'Last Name', 'value': 'Last Name'}
                                ],
                                value=col_type,
                                style={'font-size': '14px'}
                            )
                        ], width=6),
                        dbc.Col([
                            dbc.Button(
                                "Remove",
                                id={'type': 'remove-column', 'op': op_id, 'col': col_id},
                                color="danger",
                                size="sm",
                                className="mt-4"
                            )
                        ], width=2)
                    ], className="mb-2"),
                    html.Div(type_inputs, id={'type': 'col-config', 'op': op_id, 'col': col_id})
                ])
            ], className="mb-2")
            
            column_cards.append(card)
        
        return column_cards

    def _generate_batch_background(self, operations, company_name, company_sector):
        """Generate multiple operations in background thread."""
        try:
            for i, operation in enumerate(operations):
                # Update progress
                self.generation_state['current_operation'] = i + 1
                op_type = operation['type']
                self.generation_state['current_step'] = f"Processing {self._format_data_type(op_type)} operation..."
                
                # Generate based on operation type and configuration
                if op_type == 'pdf':
                    items = self._process_pdf_operation(operation, company_name, company_sector)
                elif op_type == 'text':
                    items = self._process_text_operation(operation, company_name, company_sector)
                elif op_type == 'tabular':
                    items = self._process_tabular_operation(operation, company_name, company_sector)
                else:
                    print(f"Unknown operation type: {op_type}")
                    continue
                
                # Add items to completed list
                if isinstance(items, list):
                    self.generation_state['completed_items'].extend(items)
                else:
                    self.generation_state['completed_items'].append(items)
                
                # Small delay between operations
                if i < len(operations) - 1:
                    self.generation_state['current_step'] = f"Preparing next operation..."
                    time.sleep(0.5)
            
            # Mark as complete
            self.generation_state['active'] = False
            self.generation_state['current_step'] = 'Batch complete!'
            
        except Exception as e:
            print(f"Error in batch generation: {str(e)}")
            self.generation_state['active'] = False
            self.generation_state['error'] = str(e)

    def _process_pdf_operation(self, operation, company_name, company_sector):
        """Process a PDF generation operation."""
        config = operation.get('config', {})
        doc_type = config.get('doc_type', 'policy_guide')
        description = config.get('description', 'Sample document')
        count = config.get('count', 1)
        
        items = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for i in range(count):
            enhanced_description = f"For {company_name} (a {company_sector} company): {description}"
            item = self._generate_pdf_item(enhanced_description, company_name, company_sector, f"{timestamp}_{i+1}")
            items.append(item)
        
        return items

    def _process_text_operation(self, operation, company_name, company_sector):
        """Process a text generation operation."""
        config = operation.get('config', {})
        doc_type = config.get('doc_type', 'policy_guide')
        description = config.get('description', 'Sample document')
        file_format = config.get('file_format', 'txt')
        count = config.get('count', 1)
        
        items = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for i in range(count):
            enhanced_description = f"For {company_name} (a {company_sector} company): {description}"
            item = self._generate_text_item(enhanced_description, company_name, company_sector, f"{timestamp}_{i+1}", doc_type, file_format)
            items.append(item)
        
        return items

    def _process_tabular_operation(self, operation, company_name, company_sector):
        """Process a tabular data generation operation using dbldatagen."""
        config = operation.get('config', {})
        table_name = config.get('table_name', 'sample_table')
        row_count = config.get('row_count', 1000)
        columns = config.get('columns', [])
        
        if not columns:
            # If no columns configured, skip this operation
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        return self._generate_tabular_item(table_name, row_count, columns, company_name, company_sector, timestamp)

    def _generate_documents_background(self, doc_type, description, count):
        """Generate documents in background thread with progress updates."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for i in range(count):
                # Update progress
                self.generation_state['current_doc'] = i + 1
                self.generation_state['current_step'] = f"Generating content for document {i + 1}..."
                
                # Generate content using the serving endpoint
                content = self._generate_document_content(doc_type, description, i + 1)
                
                # Update progress
                self.generation_state['current_step'] = f"Creating PDF for document {i + 1}..."
                
                # Create filename
                filename = f"{doc_type}_{timestamp}_{i+1:02d}.pdf"
                
                # Generate PDF
                pdf_path = self._create_pdf(content, filename, doc_type)
                
                # Update progress and save to volume
                self.generation_state['current_step'] = f"Saving document {i + 1} to volume..."
                
                # Save to volume and check success
                save_success = self._save_to_volume(pdf_path, filename)
                
                file_info = {
                    'filename': filename,
                    'path': pdf_path,
                    'doc_type': doc_type,
                    'size': os.path.getsize(pdf_path) if os.path.exists(pdf_path) else 0,
                    'saved_to_volume': save_success
                }
                
                self.generation_state['completed_files'].append(file_info)
                
                # Small delay between generations
                if i < count - 1:  # Don't delay after the last document
                    self.generation_state['current_step'] = f"Preparing next document..."
                    time.sleep(0.5)
            
            # Mark as complete
            self.generation_state['active'] = False
            self.generation_state['current_step'] = 'Complete!'
            
        except Exception as e:
            print(f"Error in background generation: {str(e)}")
            self.generation_state['active'] = False
            self.generation_state['error'] = str(e)

    def _generate_document_content(self, doc_type, description, doc_number):
        """Generate document content using the serving endpoint."""
        
        # Create document-specific prompts
        doc_prompts = {
            'policy_guide': f"""Create a comprehensive policy guide document with the following characteristics:
{description}

This should be document #{doc_number} in a series. Make it realistic and detailed with:
- Clear policy sections and subsections
- Specific procedures and guidelines
- Professional formatting with headers
- Realistic company policies and procedures
- Compliance and regulatory information where appropriate

Generate a complete document with multiple sections, not just an outline.""",
            
            'customer_correspondence': f"""Create a realistic customer correspondence document with these characteristics:
{description}

This should be document #{doc_number} in a series. Include:
- Realistic customer information (use fictional names and details)
- Professional business communication tone
- Specific details about products, services, or issues
- Appropriate formatting for business correspondence
- Dates, reference numbers, and other realistic details

Generate a complete correspondence document, not just a template.""",
            
            'customer_profile': f"""Create a detailed customer profile document with these characteristics:
{description}

This should be document #{doc_number} in a series. Include:
- Comprehensive customer demographics and information
- Purchase history and preferences
- Account details and status
- Contact information and communication preferences
- Notes and interactions history
- Risk assessment or credit information where relevant

Generate a complete customer profile with realistic data (use fictional information)."""
        }
        
        prompt = doc_prompts.get(doc_type, f"Create a {doc_type} document with these characteristics: {description}")
        
        messages = [
            {"role": "system", "content": "You are a professional document generator. Create realistic, detailed documents based on the user's specifications. Use fictional but realistic data. Make the documents comprehensive and well-structured. IMPORTANT: Return ONLY the document content itself, no reasoning, no metadata, no explanations - just the complete document text ready for PDF generation."},
            {"role": "user", "content": prompt + "\n\nIMPORTANT: Respond with ONLY the complete document content. Do not include any reasoning, metadata, or explanations. Start directly with the document title and content."}
        ]
        
        try:
            response = query_endpoint(self.endpoint_name, messages, max_tokens=2048)
            
            # Debug logging to understand response structure
            print(f"DEBUG: Raw response type: {type(response)}")
            if isinstance(response, dict):
                print(f"DEBUG: Response keys: {list(response.keys())}")
                if 'type' in response:
                    print(f"DEBUG: Response type field: {response['type']}")
            
            # Extract content using robust logic to handle different response types
            content = self._extract_content_safely(response)
            
            # Ensure content is always a string
            if isinstance(content, list):
                # If content is a list, join it into a string
                content = '\n\n'.join(str(item) for item in content)
            elif not isinstance(content, str):
                content = str(content)
            
            return content
                
        except Exception as e:
            print(f"Error generating content: {str(e)}")
            # Fallback content
            return f"Sample {self._format_doc_type(doc_type)} Document #{doc_number}\n\n{description}\n\nThis is a placeholder document generated due to an API error."

    def _create_pdf(self, content, filename, doc_type):
        """Create a PDF file from the generated content."""
        
        # Ensure the volume directory exists (create locally for now)
        local_dir = "./generated_documents"
        os.makedirs(local_dir, exist_ok=True)
        
        pdf_path = os.path.join(local_dir, filename)
        
        # Create PDF
        doc = SimpleDocTemplate(pdf_path, pagesize=letter)
        styles = getSampleStyleSheet()
        
        # Custom styles
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            spaceAfter=30,
            alignment=1  # Center alignment
        )
        
        body_style = ParagraphStyle(
            'CustomBody',
            parent=styles['Normal'],
            fontSize=11,
            spaceAfter=12,
            alignment=0  # Left alignment
        )
        
        # Build document
        story = []
        
        # Title
        title = f"{self._format_doc_type(doc_type)} Document"
        story.append(Paragraph(title, title_style))
        story.append(Spacer(1, 0.2*inch))
        
        # Sanitize content for PDF generation
        content = self._sanitize_content_for_pdf(content)
        
        # Content - split by paragraphs and create PDF elements
        paragraphs = content.split('\n\n')
        for para in paragraphs:
            if para.strip():
                try:
                    # Clean the paragraph text for ReportLab
                    para_text = para.strip()
                    
                    # Escape special characters that might cause ReportLab issues
                    para_text = para_text.replace('&', '&amp;')
                    para_text = para_text.replace('<', '&lt;')
                    para_text = para_text.replace('>', '&gt;')
                    
                    # Handle headers (lines that might be section titles)
                    if len(para_text) < 100 and para_text.endswith(':'):
                        story.append(Paragraph(para_text, styles['Heading2']))
                    else:
                        story.append(Paragraph(para_text, body_style))
                    story.append(Spacer(1, 0.1*inch))
                except Exception as e:
                    # If paragraph creation fails, add as plain text
                    print(f"Warning: Could not create paragraph, adding as plain text: {str(e)}")
                    # Convert to safe text and add as simple paragraph
                    safe_text = para.strip().replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    try:
                        story.append(Paragraph(safe_text, body_style))
                        story.append(Spacer(1, 0.1*inch))
                    except:
                        # Last resort: skip this paragraph
                        print(f"Skipping problematic paragraph: {para[:100]}...")
                        continue
        
        # Build PDF
        doc.build(story)
        
        return pdf_path
    
    def _create_txt(self, content, filepath, company_name):
        """Create a text file with the generated content."""
        try:
            # Clean the content for text format
            clean_content = self._sanitize_content_for_text(content)
            
            # Add header information
            header = f"{company_name} - Generated Document\n"
            header += "=" * len(header.strip()) + "\n\n"
            header += f"Generated on: {datetime.now().strftime('%Y-%m-%d at %I:%M %p')}\n\n"
            
            # Combine header and content
            full_content = header + clean_content
            
            # Write to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(full_content)
                
        except Exception as e:
            print(f"Error creating text file: {str(e)}")
            raise
    
    def _create_docx(self, content, filepath, company_name):
        """Create a Word document with the generated content."""
        try:
            # Clean the content for docx format
            clean_content = self._sanitize_content_for_text(content)
            
            # Create document
            doc = Document()
            
            # Add title
            title = doc.add_heading(f'{company_name} - Generated Document', 0)
            
            # Add generation timestamp
            timestamp_para = doc.add_paragraph()
            timestamp_para.add_run(f'Generated on: {datetime.now().strftime("%Y-%m-%d at %I:%M %p")}').italic = True
            
            # Add a line break
            doc.add_paragraph()
            
            # Process content - split by paragraphs and handle formatting
            paragraphs = clean_content.split('\n\n')
            
            for para_text in paragraphs:
                if para_text.strip():
                    # Check if it looks like a heading (starts with #, all caps, or ends with :)
                    if (para_text.strip().startswith('#') or 
                        para_text.strip().isupper() or 
                        para_text.strip().endswith(':')):
                        # Add as heading
                        heading_text = para_text.strip().lstrip('#').strip()
                        doc.add_heading(heading_text, level=1)
                    else:
                        # Add as regular paragraph
                        para = doc.add_paragraph()
                        # Handle basic formatting
                        lines = para_text.strip().split('\n')
                        for i, line in enumerate(lines):
                            if i > 0:
                                para.add_run('\n')
                            para.add_run(line.strip())
            
            # Save document
            doc.save(filepath)
            
        except Exception as e:
            print(f"Error creating Word document: {str(e)}")
            raise
    
    def _sanitize_content_for_text(self, content):
        """Sanitize content for text/docx format."""
        if not isinstance(content, str):
            content = str(content)
        
        # Remove HTML/XML tags
        import re
        content = re.sub(r'<[^>]+>', '', content)
        
        # Clean up excessive whitespace
        content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
        content = re.sub(r'[ \t]+', ' ', content)
        
        # Remove markdown table formatting for text
        lines = content.split('\n')
        clean_lines = []
        for line in lines:
            # Skip markdown table separator lines
            if re.match(r'^\s*\|[\s\-\|]*\|\s*$', line):
                continue
            # Convert table rows to simple text
            if '|' in line and not line.strip().startswith('|'):
                # This might be a table row, convert to simple format
                parts = [part.strip() for part in line.split('|') if part.strip()]
                if len(parts) > 1:
                    line = ' - '.join(parts)
            clean_lines.append(line)
        
        content = '\n'.join(clean_lines)
        
        # Ensure content doesn't start with newlines
        content = content.strip()
        
        return content

    def _save_to_volume(self, local_path, filename):
        """Save file to Databricks volume using WorkspaceClient pattern from cookbook."""
        import io
        from databricks.sdk import WorkspaceClient
        
        try:
            w = WorkspaceClient()
            
            # Read file into bytes
            print(f"Reading file: {local_path}")
            with open(local_path, "rb") as f:
                file_bytes = f.read()
            binary_data = io.BytesIO(file_bytes)
            
            # Construct volume file path
            # self.volume_path is "/Volumes/conor_smith/synthetic_data_app/synthetic_data_volume"
            volume_file_path = f"{self.volume_path}/{filename}"
            
            print(f"Uploading to volume: {volume_file_path}")
            w.files.upload(volume_file_path, binary_data, overwrite=True)
            
            print(f"✅ SUCCESS: {filename} uploaded to volume ({len(file_bytes):,} bytes)")
            return True
            
        except ImportError as e:
            print(f"❌ ERROR: Databricks SDK not available - {str(e)}")
            print("Install with: pip install databricks-sdk")
            return False
        except Exception as e:
            print(f"❌ ERROR: Upload failed - {str(e)}")
            print(f"   Source: {local_path}")
            print(f"   Target: {self.volume_path}/{filename}")
            return False

    def _format_doc_type(self, doc_type):
        """Format document type for display."""
        type_map = {
            'policy_guide': 'Policy Guide',
            'customer_correspondence': 'Customer Correspondence',
            'customer_profile': 'Customer Profile'
        }
        return type_map.get(doc_type, doc_type.replace('_', ' ').title())

    def _sanitize_content_for_pdf(self, content):
        """Clean content to make it safe for PDF generation."""
        if not isinstance(content, str):
            content = str(content)
        
        import re
        
        # Remove HTML/XML tags
        content = re.sub(r'<[^>]+>', '', content)
        
        # Convert markdown tables to readable text
        lines = content.split('\n')
        cleaned_lines = []
        in_table = False
        
        for line in lines:
            # Detect table rows (lines with multiple | characters)
            if '|' in line and line.count('|') >= 2:
                in_table = True
                # Clean up table formatting
                cells = [cell.strip() for cell in line.split('|') if cell.strip()]
                if cells:  # Skip empty rows
                    # Skip header separator rows (like |------|-----|)
                    if not all(cell.replace('-', '').replace(' ', '') == '' for cell in cells):
                        # Format as readable text
                        if len(cells) >= 2:
                            cleaned_lines.append(f"{cells[0]}: {' '.join(cells[1:])}")
                        else:
                            cleaned_lines.append(' '.join(cells))
            else:
                if in_table and line.strip() == '':
                    in_table = False
                    cleaned_lines.append('')  # Add space after table
                elif not in_table:
                    cleaned_lines.append(line)
        
        content = '\n'.join(cleaned_lines)
        
        # Clean up special characters that might cause issues
        content = content.replace('\u2022', '•')  # Bullet points
        content = content.replace('\u2011', '-')  # Non-breaking hyphens
        content = content.replace('\u2013', '-')  # En dashes
        content = content.replace('\u2014', '--') # Em dashes
        
        # Remove any remaining HTML entities
        content = re.sub(r'&[a-zA-Z0-9#]+;', '', content)
        
        # Clean up excessive whitespace
        content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)  # Multiple blank lines
        content = re.sub(r'[ \t]+', ' ', content)  # Multiple spaces/tabs
        
        return content.strip()

    def _extract_content_safely(self, response):
        """Safely extract content from various response formats."""
        if isinstance(response, dict):
            # Handle reasoning/summary structure first
            if response.get('type') == 'reasoning' and 'summary' in response:
                # This is metadata, not the actual content - skip it
                print("Detected reasoning metadata, looking for actual content...")
                return "Content extraction failed - received metadata instead of document content"
            
            # Try common keys in order of preference
            for key in ["content", "text", "message", "summary"]:
                if key in response:
                    value = response[key]
                    # Handle nested structures
                    if isinstance(value, dict):
                        if "content" in value:
                            return value["content"]
                        elif "text" in value:
                            return value["text"]
                        else:
                            # Skip metadata objects
                            continue
                    elif isinstance(value, list):
                        # If it's a list, try to extract content from each item
                        extracted_items = []
                        for item in value:
                            if isinstance(item, dict):
                                # Skip metadata objects like reasoning summaries
                                if item.get('type') in ['summary_text', 'reasoning']:
                                    continue
                                elif "content" in item:
                                    extracted_items.append(item["content"])
                                elif "text" in item:
                                    extracted_items.append(item["text"])
                                else:
                                    # Only include if it looks like actual content
                                    item_str = str(item)
                                    if len(item_str) > 100 and not item_str.startswith("{'type'"):
                                        extracted_items.append(item_str)
                            else:
                                extracted_items.append(str(item))
                        return '\n\n'.join(extracted_items) if extracted_items else str(value)
                    else:
                        # Only return string values that look like actual content
                        value_str = str(value)
                        if len(value_str) > 50 and not value_str.startswith("{'type'"):
                            return value_str
            
            # If none of the expected keys exist, return string representation
            response_str = str(response)
            # But avoid returning obvious metadata
            if response_str.startswith("{'type': 'reasoning'"):
                return "Error: Received reasoning metadata instead of document content"
            return response_str
            
        elif isinstance(response, list):
            # If response is directly a list, extract content from each item
            extracted_items = []
            for item in response:
                if isinstance(item, dict):
                    # Skip metadata items
                    if item.get('type') in ['reasoning', 'summary_text']:
                        continue
                    content = self._extract_content_safely(item)  # Recursive call
                    if content and not content.startswith("Error:") and len(content) > 50:
                        extracted_items.append(content)
                else:
                    extracted_items.append(str(item))
            return '\n\n'.join(extracted_items) if extracted_items else str(response)
        else:
            return str(response)

    def _add_custom_css(self):
        """Add custom CSS for animations and styling."""
        custom_css = '''
        @import url('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css');
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
        
        body {
            font-family: 'DM Sans', sans-serif;
            background-color: #F9F7F4;
        }
        
        .fa-spinner {
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .generate-button:disabled {
            opacity: 0.6;
            cursor: not-allowed;
        }
        
        .progress-container {
            border-radius: 10px;
            overflow: hidden;
        }
        
        .alert-info {
            border-left: 4px solid #17a2b8;
        }
        
        .alert-success {
            border-left: 4px solid #28a745;
        }
        
        .alert-danger {
            border-left: 4px solid #dc3545;
        }
        
        .document-config-card {
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            border-radius: 10px;
        }
        
        .form-label {
            color: #1B3139;
            font-weight: 600;
        }
        
        .progress {
            height: 8px;
            border-radius: 4px;
        }
        
        .progress-bar {
            background: linear-gradient(90deg, #17a2b8, #28a745);
        }
        '''
        
        self.app.index_string = self.app.index_string.replace(
            '</head>',
            f'<style>{custom_css}</style></head>'
        )
    
    def _format_data_type(self, data_type):
        """Format data type for display."""
        type_map = {
            'pdf': 'PDF Document',
            'text': 'Text Document',
            'tabular': 'Tabular Data'
        }
        return type_map.get(data_type, data_type.title())
    
    def _format_items_display(self, items, data_type):
        """Format items for history display."""
        if not items:
            return html.P("No items generated", className="text-muted small")
        
        item_elements = []
        for item in items:
            if data_type == 'pdf':
                icon = "📄"
                size_info = f" ({item.get('size', 0):,} bytes)" if item.get('size') else ""
                volume_status = " ✅" if item.get('saved_to_volume') else " 📁"
            elif data_type == 'text':
                icon = "📝"
                size_info = f" ({item.get('size', 0):,} chars)" if item.get('size') else ""
                volume_status = ""
            elif data_type == 'tabular':
                icon = "📊"
                size_info = f" ({item.get('size', 0):,} bytes)" if item.get('size') else ""
                volume_status = ""
            else:
                icon = "📄"
                size_info = ""
                volume_status = ""
            
            item_elements.append(
                html.Li([
                    f"{icon} {item.get('filename', 'Unknown file')}",
                    html.Small(size_info + volume_status, className="text-muted")
                ], className="small")
            )
        
        return html.Ul(item_elements, className="mb-0")
    
    def _generate_data_background(self, data_type, description, company_name, company_sector):
        """Generate data in background thread with progress updates."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Update progress
            self.generation_state['current_item'] = 1
            self.generation_state['current_step'] = f"Generating {self._format_data_type(data_type)}..."
            
            if data_type == 'pdf':
                # Generate PDF (existing functionality)
                item_info = self._generate_pdf_item(description, company_name, company_sector, timestamp)
            elif data_type == 'text':
                # Generate text document
                item_info = self._generate_text_item(description, company_name, company_sector, timestamp)
            elif data_type == 'tabular':
                # Generate tabular data
                item_info = self._generate_tabular_item(description, company_name, company_sector, timestamp)
            else:
                raise Exception(f"Unknown data type: {data_type}")
            
            self.generation_state['completed_items'].append(item_info)
            
            # Mark as complete
            self.generation_state['active'] = False
            self.generation_state['current_step'] = 'Complete!'
            
        except Exception as e:
            print(f"Error in background generation: {str(e)}")
            self.generation_state['active'] = False
            self.generation_state['error'] = str(e)

    def _generate_pdf_item(self, description, company_name, company_sector, timestamp):
        """Generate a single PDF item."""
        # Enhanced prompt with company context
        enhanced_description = f"For {company_name} (a {company_sector} company): {description}"
        
        # Generate content using the serving endpoint
        content = self._generate_document_content('policy_guide', enhanced_description, 1)
        
        # Update progress
        self.generation_state['current_step'] = f"Creating PDF..."
        
        # Create filename
        filename = f"synthetic_pdf_{timestamp}.pdf"
        
        # Generate PDF
        pdf_path = self._create_pdf(content, filename, 'pdf')
        
        # Update progress and save to volume
        self.generation_state['current_step'] = f"Saving PDF to volume..."
        
        try:
            self._save_to_volume(pdf_path, filename)
            save_success = True
        except Exception as e:
            save_success = False
            print(f"Failed to save PDF to volume: {str(e)}")
            # For iterative generation, we don't fail completely on volume save errors
        
        return {
            'type': 'pdf',
            'filename': filename,
            'path': pdf_path,
            'description': description,
            'company_name': company_name,
            'company_sector': company_sector,
            'size': os.path.getsize(pdf_path) if os.path.exists(pdf_path) else 0,
            'saved_to_volume': save_success
        }

    def _generate_text_item(self, description, company_name, company_sector, timestamp, doc_type, file_format):
        """Generate a single text document with LLM content."""
        try:
            # Generate content using LLM (similar to PDF generation)
            self.generation_state['current_step'] = f"Generating {file_format.upper()} content..."
            
            # Enhance description with company context
            enhanced_description = f"""For {company_name} (a {company_sector} company): {description}
            
Company Context:
- Company Name: {company_name}
- Industry Sector: {company_sector}

Please incorporate this company information naturally throughout the document to make it specific and realistic for this organization."""
            
            # Extract doc number from timestamp for document series numbering
            doc_number = int(timestamp.split('_')[-1]) if '_' in timestamp else 1
            content = self._generate_document_content(doc_type, enhanced_description, doc_number)
            
            # Create filename based on format
            if file_format == 'docx':
                filename = f"{doc_type}_{timestamp}.docx"
                extension = 'docx'
            else:
                filename = f"{doc_type}_{timestamp}.txt"
                extension = 'txt'
                
            # Create local file
            os.makedirs('./generated_documents', exist_ok=True)
            local_path = f"./generated_documents/{filename}"
            
            if file_format == 'docx':
                self._create_docx(content, local_path, company_name)
            else:
                self._create_txt(content, local_path, company_name)
            
            # Save to volume
            self.generation_state['current_step'] = f"Saving {extension.upper()} to volume..."
            volume_success = self._save_to_volume(local_path, filename)
            
            return {
                'type': 'text',
                'description': description,
                'filename': filename,
                'format': file_format,
                'doc_type': doc_type,
                'path': local_path,
                'timestamp': timestamp,
                'company_name': company_name,
                'company_sector': company_sector,
                'saved_to_volume': volume_success,
                'size': os.path.getsize(local_path) if os.path.exists(local_path) else 0
            }
            
        except Exception as e:
            print(f"Error generating text document: {str(e)}")
            return {
                'type': 'text',
                'description': description,
                'filename': f"error_{timestamp}.txt",
                'format': file_format,
                'error': str(e),
                'timestamp': timestamp,
                'company_name': company_name,
                'company_sector': company_sector,
                'saved_to_volume': False,
                'path': None,
                'size': 0
            }

    def _generate_tabular_item(self, table_name, row_count, columns, company_name, company_sector, timestamp):
        """Generate tabular data using dbldatagen."""
        try:
            self.generation_state['current_step'] = f"Generating {row_count} rows of tabular data..."
            
            # Get or create Spark session - try to get existing session first
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                if spark is None:
                    # Create a new Spark session if none exists
                    spark = SparkSession.builder \
                        .appName("SyntheticDataGenerator") \
                        .config("spark.sql.shuffle.partitions", "8") \
                        .getOrCreate()
            except ImportError:
                # If PySpark is not available, create a simple CSV as fallback
                return self._generate_tabular_fallback(table_name, row_count, columns, company_name, company_sector, timestamp)
            
            # Set partition parameters
            partitions_requested = min(8, max(1, row_count // 1000))  # 1 partition per 1000 rows, max 8
            spark.conf.set("spark.sql.shuffle.partitions", str(partitions_requested))
            
            # Create DataGenerator
            data_gen = DataGenerator(spark, rows=row_count, partitions=partitions_requested)
            
            # Add columns based on configuration
            for col in columns:
                col_name = col.get('name', 'unnamed_column')
                col_type = col.get('data_type', 'Integer')
                
                if col_type == 'Integer':
                    min_val = col.get('min_value', 1)
                    max_val = col.get('max_value', 100)
                    data_gen = data_gen.withColumn(col_name, "integer", minValue=min_val, maxValue=max_val)
                elif col_type == 'First Name':
                    data_gen = data_gen.withColumn(col_name, text=fakerText("first_name"))
                elif col_type == 'Last Name':
                    data_gen = data_gen.withColumn(col_name, text=fakerText("last_name"))
            
            # Add company context columns
            data_gen = data_gen.withColumn("company_name", "string", values=[company_name])
            data_gen = data_gen.withColumn("company_sector", "string", values=[company_sector])
            
            # Generate the DataFrame
            self.generation_state['current_step'] = f"Building DataFrame with {row_count} rows..."
            df = data_gen.build()
            
            # Show first 5 rows for preview
            self.generation_state['current_step'] = f"Collecting preview data..."
            preview_data = df.limit(5).toPandas()
            
            # Save to CSV
            filename = f"{table_name}_{timestamp}.csv"
            local_dir = "./generated_documents"
            os.makedirs(local_dir, exist_ok=True)
            csv_path = os.path.join(local_dir, filename)
            
            self.generation_state['current_step'] = f"Saving to CSV file..."
            # Convert full dataset to Pandas and save
            pandas_df = df.toPandas()
            pandas_df.to_csv(csv_path, index=False)
            
            # Save to volume
            self.generation_state['current_step'] = f"Saving CSV to volume..."
            volume_success = self._save_to_volume(csv_path, filename)
            
            return {
                'type': 'tabular',
                'table_name': table_name,
                'filename': filename,
                'path': csv_path,
                'row_count': row_count,
                'column_count': len(columns) + 2,  # +2 for company columns
                'columns': [col.get('name', 'unnamed') for col in columns] + ['company_name', 'company_sector'],
                'preview_data': preview_data.to_html(classes='table table-striped table-sm', index=False) if not preview_data.empty else "No preview available",
                'timestamp': timestamp,
                'company_name': company_name,
                'company_sector': company_sector,
                'size': os.path.getsize(csv_path) if os.path.exists(csv_path) else 0,
                'saved_to_volume': volume_success
            }
            
        except Exception as e:
            print(f"Error generating tabular data: {str(e)}")
            # Fallback to simple CSV generation
            return self._generate_tabular_fallback(table_name, row_count, columns, company_name, company_sector, timestamp)

    def _generate_tabular_fallback(self, table_name, row_count, columns, company_name, company_sector, timestamp):
        """Fallback method to generate tabular data without Spark."""
        try:
            import pandas as pd
            import random
            from faker import Faker
            fake = Faker()
            
            self.generation_state['current_step'] = f"Generating {row_count} rows (fallback mode)..."
            
            # Create data dictionary
            data = {}
            
            # Generate data for each column
            for col in columns:
                col_name = col.get('name', 'unnamed_column')
                col_type = col.get('data_type', 'Integer')
                
                if col_type == 'Integer':
                    min_val = col.get('min_value', 1)
                    max_val = col.get('max_value', 100)
                    data[col_name] = [random.randint(min_val, max_val) for _ in range(row_count)]
                elif col_type == 'First Name':
                    data[col_name] = [fake.first_name() for _ in range(row_count)]
                elif col_type == 'Last Name':
                    data[col_name] = [fake.last_name() for _ in range(row_count)]
            
            # Add company context columns
            data['company_name'] = [company_name] * row_count
            data['company_sector'] = [company_sector] * row_count
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Save to CSV
            filename = f"{table_name}_{timestamp}.csv"
            local_dir = "./generated_documents"
            os.makedirs(local_dir, exist_ok=True)
            csv_path = os.path.join(local_dir, filename)
            
            df.to_csv(csv_path, index=False)
            
            # Get preview (first 5 rows)
            preview_data = df.head(5)
            
            # Save to volume
            volume_success = self._save_to_volume(csv_path, filename)
            
            return {
                'type': 'tabular',
                'table_name': table_name,
                'filename': filename,
                'path': csv_path,
                'row_count': row_count,
                'column_count': len(columns) + 2,
                'columns': [col.get('name', 'unnamed') for col in columns] + ['company_name', 'company_sector'],
                'preview_data': preview_data.to_html(classes='table table-striped table-sm', index=False),
                'timestamp': timestamp,
                'company_name': company_name,
                'company_sector': company_sector,
                'size': os.path.getsize(csv_path),
                'saved_to_volume': volume_success
            }
            
        except Exception as e:
            print(f"Error in fallback tabular generation: {str(e)}")
            return {
                'type': 'tabular',
                'table_name': table_name,
                'filename': f"error_{timestamp}.csv",
                'error': str(e),
                'timestamp': timestamp,
                'company_name': company_name,
                'company_sector': company_sector,
                'saved_to_volume': False,
                'path': None,
                'size': 0
            }
