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
                    
                    # Check which files were successfully saved to volume
                    volume_saved = [f for f in self.generation_state['completed_files'] if f.get('saved_to_volume', False)]
                    local_only = [f for f in self.generation_state['completed_files'] if not f.get('saved_to_volume', False)]
                    
                    # Build success message
                    success_items = [
                        html.H5("✅ Generation Complete!", className="mb-2"),
                        html.P(f"Successfully generated {len(self.generation_state['completed_files'])} document(s) in {elapsed_time:.1f} seconds")
                    ]
                    
                    if volume_saved:
                        success_items.extend([
                            html.P("📁 Files saved to volume:", className="mb-2 mt-3 fw-bold"),
                            html.Ul([
                                html.Li([
                                    file_info['filename'], 
                                    html.Small(f" ({file_info['size']} bytes)", className="text-muted")
                                ]) for file_info in volume_saved
                            ]),
                            html.P([
                                "Volume path: ",
                                html.Code(self.volume_path)
                            ], className="mb-0 small text-muted")
                        ])
                    
                    if local_only:
                        success_items.extend([
                            html.Hr(),
                            html.P("⚠️ Files created locally (manual copy needed):", className="mb-2 fw-bold text-warning"),
                            html.Ul([
                                html.Li([
                                    file_info['filename'],
                                    html.Small(f" → {file_info['path']}", className="text-muted")
                                ]) for file_info in local_only
                            ])
                        ])
                    
                    success_message = dbc.Alert(success_items, color="success")
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
            {"role": "system", "content": "You are a professional document generator. Create realistic, detailed documents based on the user's specifications. Use fictional but realistic data. Make the documents comprehensive and well-structured."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response = query_endpoint(self.endpoint_name, messages, max_tokens=2048)
            
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
        """Save file to Databricks volume using direct file operations."""
        import shutil
        import os
        
        # Target volume path
        volume_file_path = f"{self.volume_path}/{filename}"
        
        try:
            # Ensure the volume directory exists
            volume_dir = os.path.dirname(volume_file_path)
            os.makedirs(volume_dir, exist_ok=True)
            
            # Direct file copy to volume - this works when volumes are properly mounted
            print(f"Copying {local_path} to {volume_file_path}")
            shutil.copy2(local_path, volume_file_path)
            
            # Verify the copy worked
            if os.path.exists(volume_file_path):
                file_size = os.path.getsize(volume_file_path)
                print(f"✅ SUCCESS: {filename} copied to volume ({file_size:,} bytes)")
                return True
            else:
                print(f"❌ FAILED: File not found at {volume_file_path} after copy")
                return False
                
        except Exception as e:
            print(f"❌ ERROR: Failed to copy {filename} to volume - {str(e)}")
            print(f"   Source: {local_path}")
            print(f"   Target: {volume_file_path}")
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
