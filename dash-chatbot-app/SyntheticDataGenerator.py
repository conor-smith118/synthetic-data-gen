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
        # Start generation callback
        @self.app.callback(
            Output('progress-interval', 'disabled'),
            Output('progress-store', 'data'),
            Output('generate-button', 'disabled'),
            Input('generate-button', 'n_clicks'),
            State('document-type-dropdown', 'value'),
            State('document-description', 'value'),
            State('document-count-slider', 'value'),
            prevent_initial_call=True
        )
        def start_generation(n_clicks, doc_type, description, count):
            if not n_clicks or not description:
                return dash.no_update, dash.no_update, dash.no_update
            
            # Reset and start generation state
            self.generation_state = {
                'active': True,
                'current_doc': 0,
                'total_docs': count,
                'current_step': 'Initializing...',
                'completed_files': [],
                'error': None,
                'start_time': time.time(),
                'doc_type': doc_type,
                'description': description
            }
            
            # Start generation in background thread
            threading.Thread(
                target=self._generate_documents_background, 
                args=(doc_type, description, count),
                daemon=True
            ).start()
            
            # Enable interval and disable button
            return False, {'status': 'started'}, True

        # Progress update callback
        @self.app.callback(
            Output('generation-status', 'children'),
            Output('progress-interval', 'disabled', allow_duplicate=True),
            Output('generate-button', 'disabled', allow_duplicate=True),
            Output('generation-store', 'data'),
            Input('progress-interval', 'n_intervals'),
            prevent_initial_call=True
        )
        def update_progress(n_intervals):
            if not self.generation_state['active'] and self.generation_state['current_doc'] == 0:
                return dash.no_update, dash.no_update, dash.no_update, dash.no_update
            
            # Calculate progress
            progress_percent = 0
            if self.generation_state['total_docs'] > 0:
                progress_percent = (self.generation_state['current_doc'] / self.generation_state['total_docs']) * 100
            
            # Check if generation is complete
            if not self.generation_state['active']:
                if self.generation_state['error']:
                    # Error state
                    error_message = dbc.Alert([
                        html.H5("❌ Generation Failed", className="mb-2"),
                        html.P(f"Error: {self.generation_state['error']}")
                    ], color="danger")
                    return error_message, True, False, {'status': 'error', 'error': self.generation_state['error']}
                else:
                    # Success state
                    elapsed_time = time.time() - self.generation_state['start_time']
                    success_message = dbc.Alert([
                        html.H5("✅ Generation Complete!", className="mb-2"),
                        html.P(f"Successfully generated {len(self.generation_state['completed_files'])} document(s) in {elapsed_time:.1f} seconds"),
                        html.Ul([
                            html.Li(file_info['filename']) for file_info in self.generation_state['completed_files']
                        ]),
                        html.Hr(),
                        html.P([
                            "Files saved to: ",
                            html.Code(self.volume_path)
                        ], className="mb-0")
                    ], color="success")
                    return success_message, True, False, {'status': 'complete', 'files': self.generation_state['completed_files']}
            
            # Active generation state
            status_message = dbc.Alert([
                html.Div([
                    html.Div([
                        html.I(className="fas fa-spinner fa-spin me-2"),
                        html.H5("🔄 Generating Documents...", className="d-inline mb-0")
                    ], className="d-flex align-items-center mb-3"),
                    
                    html.P(f"Document {self.generation_state['current_doc']} of {self.generation_state['total_docs']}", className="mb-2"),
                    html.P(f"Current step: {self.generation_state['current_step']}", className="mb-3 text-muted"),
                    
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
                
                # Update progress
                self.generation_state['current_step'] = f"Saving document {i + 1} to volume..."
                
                file_info = {
                    'filename': filename,
                    'path': pdf_path,
                    'doc_type': doc_type,
                    'size': os.path.getsize(pdf_path) if os.path.exists(pdf_path) else 0
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
            {"role": "system", "content": "You are a professional document generator. Create realistic, detailed documents based on the user's specifications. Use fictional but realistic data. Make the documents comprehensive and well-structured."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response = query_endpoint(self.endpoint_name, messages, max_tokens=2048)
            
            # Debug logging
            print(f"Raw response type: {type(response)}")
            print(f"Raw response: {response}")
            
            # Extract content using robust logic to handle different response types
            content = self._extract_content_safely(response)
            
            # Ensure content is always a string
            if isinstance(content, list):
                print(f"Content is list, converting to string: {content}")
                # If content is a list, join it into a string
                content = '\n\n'.join(str(item) for item in content)
            elif not isinstance(content, str):
                print(f"Content is not string ({type(content)}), converting: {content}")
                content = str(content)
            
            print(f"Final content type: {type(content)}")
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
        
        # Ensure content is a string before processing
        if not isinstance(content, str):
            content = str(content)
        
        # Content - split by paragraphs and create PDF elements
        paragraphs = content.split('\n\n')
        for para in paragraphs:
            if para.strip():
                # Handle headers (lines that might be section titles)
                if len(para) < 100 and para.strip().endswith(':'):
                    story.append(Paragraph(para.strip(), styles['Heading2']))
                else:
                    story.append(Paragraph(para.strip(), body_style))
                story.append(Spacer(1, 0.1*inch))
        
        # Build PDF
        doc.build(story)
        
        # Copy to Databricks volume (simulate for now - would need Databricks context)
        # In a real Databricks environment, you would copy the file to the volume here
        self._save_to_volume(pdf_path, filename)
        
        return pdf_path

    def _save_to_volume(self, local_path, filename):
        """Save file to Databricks volume. This is a placeholder implementation."""
        # In a real Databricks environment, you would use:
        # dbutils.fs.cp(local_path, f"{self.volume_path}/{filename}")
        
        # For now, just print the action
        print(f"Would save {filename} to {self.volume_path}/{filename}")
        
        # In development, we'll just keep the local copy
        return True

    def _format_doc_type(self, doc_type):
        """Format document type for display."""
        type_map = {
            'policy_guide': 'Policy Guide',
            'customer_correspondence': 'Customer Correspondence',
            'customer_profile': 'Customer Profile'
        }
        return type_map.get(doc_type, doc_type.replace('_', ' ').title())

    def _extract_content_safely(self, response):
        """Safely extract content from various response formats."""
        if isinstance(response, dict):
            # Try common keys in order of preference
            for key in ["content", "summary", "text", "message"]:
                if key in response:
                    value = response[key]
                    # Handle nested structures
                    if isinstance(value, dict) and "content" in value:
                        return value["content"]
                    elif isinstance(value, list):
                        # If it's a list, try to extract content from each item
                        extracted_items = []
                        for item in value:
                            if isinstance(item, dict):
                                if "content" in item:
                                    extracted_items.append(item["content"])
                                elif "text" in item:
                                    extracted_items.append(item["text"])
                                else:
                                    extracted_items.append(str(item))
                            else:
                                extracted_items.append(str(item))
                        return '\n\n'.join(extracted_items) if extracted_items else str(value)
                    else:
                        return value
            
            # If none of the expected keys exist, return string representation
            return str(response)
        elif isinstance(response, list):
            # If response is directly a list, extract content from each item
            extracted_items = []
            for item in response:
                if isinstance(item, dict):
                    content = self._extract_content_safely(item)  # Recursive call
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
