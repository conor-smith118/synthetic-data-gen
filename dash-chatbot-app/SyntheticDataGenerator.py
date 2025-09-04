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
             Input({'type': 'text-type', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'text-words', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'tabular-description', 'index': dash.dependencies.ALL}, 'value'),
             Input({'type': 'tabular-type', 'index': dash.dependencies.ALL}, 'value'),
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
                        elif comp_type == 'text-type':
                            op['config']['text_type'] = new_value
                        elif comp_type == 'text-words':
                            op['config']['word_count'] = new_value
                        elif comp_type == 'tabular-description':
                            op['config']['description'] = new_value
                        elif comp_type == 'tabular-type':
                            op['config']['data_type'] = new_value
                        elif comp_type == 'tabular-rows':
                            op['config']['row_count'] = new_value
                        
                        # Mark as configured if description is provided
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
                html.Label("Text Type:", className="form-label fw-bold"),
                dcc.Dropdown(
                    id={'type': 'text-type', 'index': op_id},
                    options=[
                        {'label': 'Marketing Copy', 'value': 'marketing'},
                        {'label': 'Product Description', 'value': 'product'},
                        {'label': 'Email Template', 'value': 'email'},
                        {'label': 'Blog Post', 'value': 'blog'}
                    ],
                    value=config.get('text_type', 'marketing'),
                    className="mb-3"
                ),
                html.Label("Description:", className="form-label fw-bold"),
                dbc.Textarea(
                    id={'type': 'text-description', 'index': op_id},
                    placeholder="Describe the text content you want to generate...",
                    value=config.get('description', ''),
                    rows=3,
                    className="mb-3"
                ),
                html.Label("Word Count Target:", className="form-label fw-bold"),
                dcc.Slider(
                    id={'type': 'text-words', 'index': op_id},
                    min=100,
                    max=2000,
                    step=100,
                    value=config.get('word_count', 500),
                    marks={i: f"{i}w" for i in range(100, 2001, 500)},
                    className="mb-3"
                )
            ]
        elif op_type == 'tabular':
            config_inputs = [
                html.Label("Data Type:", className="form-label fw-bold"),
                dcc.Dropdown(
                    id={'type': 'tabular-type', 'index': op_id},
                    options=[
                        {'label': 'Customer Data', 'value': 'customers'},
                        {'label': 'Sales Data', 'value': 'sales'},
                        {'label': 'Product Data', 'value': 'products'},
                        {'label': 'Employee Data', 'value': 'employees'}
                    ],
                    value=config.get('data_type', 'customers'),
                    className="mb-3"
                ),
                html.Label("Description:", className="form-label fw-bold"),
                dbc.Textarea(
                    id={'type': 'tabular-description', 'index': op_id},
                    placeholder="Describe the tabular data you want to generate...",
                    value=config.get('description', ''),
                    rows=3,
                    className="mb-3"
                ),
                html.Label("Number of Rows:", className="form-label fw-bold"),
                dcc.Slider(
                    id={'type': 'tabular-rows', 'index': op_id},
                    min=10,
                    max=1000,
                    step=10,
                    value=config.get('row_count', 100),
                    marks={i: str(i) for i in range(10, 1001, 200)},
                    className="mb-3"
                )
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
        """Process a text generation operation (placeholder)."""
        config = operation.get('config', {})
        text_type = config.get('text_type', 'marketing')
        description = config.get('description', 'Sample text')
        word_count = config.get('word_count', 500)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        enhanced_description = f"For {company_name} (a {company_sector} company): {description} (target: {word_count} words)"
        
        return self._generate_text_item(enhanced_description, company_name, company_sector, timestamp)

    def _process_tabular_operation(self, operation, company_name, company_sector):
        """Process a tabular data generation operation (placeholder)."""
        config = operation.get('config', {})
        data_type = config.get('data_type', 'customers')
        description = config.get('description', 'Sample data')
        row_count = config.get('row_count', 100)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        enhanced_description = f"For {company_name} (a {company_sector} company): {description} ({row_count} rows)"
        
        return self._generate_tabular_item(enhanced_description, company_name, company_sector, timestamp)

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

    def _generate_text_item(self, description, company_name, company_sector, timestamp):
        """Generate a text item (placeholder implementation)."""
        self.generation_state['current_step'] = f"Generating text content..."
        
        # Enhanced prompt with company context
        enhanced_description = f"For {company_name} (a {company_sector} company): {description}"
        
        # For now, create a simple text file
        filename = f"synthetic_text_{timestamp}.txt"
        local_dir = "./generated_documents"
        os.makedirs(local_dir, exist_ok=True)
        text_path = os.path.join(local_dir, filename)
        
        with open(text_path, 'w') as f:
            f.write(f"PLACEHOLDER TEXT DOCUMENT\n")
            f.write(f"Company: {company_name}\n")
            f.write(f"Sector: {company_sector}\n")
            f.write(f"Description: {enhanced_description}\n")
            f.write(f"Generated: {datetime.now()}\n\n")
            f.write("This is a placeholder implementation for text generation.\n")
            f.write("Future versions will use the LLM to generate rich text content.")
        
        return {
            'type': 'text',
            'filename': filename,
            'path': text_path,
            'description': description,
            'company_name': company_name,
            'company_sector': company_sector,
            'size': os.path.getsize(text_path),
            'saved_to_volume': False  # Placeholder doesn't save to volume
        }

    def _generate_tabular_item(self, description, company_name, company_sector, timestamp):
        """Generate tabular data (placeholder implementation)."""
        self.generation_state['current_step'] = f"Generating tabular data..."
        
        # For now, create a simple CSV file
        filename = f"synthetic_tabular_{timestamp}.csv"
        local_dir = "./generated_documents"
        os.makedirs(local_dir, exist_ok=True)
        csv_path = os.path.join(local_dir, filename)
        
        with open(csv_path, 'w') as f:
            f.write("column1,column2,column3,company,sector,description\n")
            f.write(f"sample_data_1,value_1,123,{company_name},{company_sector},\"{description}\"\n")
            f.write(f"sample_data_2,value_2,456,{company_name},{company_sector},\"{description}\"\n")
            f.write(f"sample_data_3,value_3,789,{company_name},{company_sector},\"{description}\"\n")
        
        return {
            'type': 'tabular',
            'filename': filename,
            'path': csv_path,
            'description': description,
            'company_name': company_name,
            'company_sector': company_sector,
            'size': os.path.getsize(csv_path),
            'saved_to_volume': False  # Placeholder doesn't save to volume
        }
