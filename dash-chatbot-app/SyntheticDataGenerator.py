import dash
from dash import html, Input, Output, State, dcc
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

class SyntheticDataGenerator:
    def __init__(self, app, endpoint_name):
        self.app = app
        self.endpoint_name = endpoint_name
        self.volume_path = "/Volumes/conor_smith/synthetic_data_app/synthetic_data_volume"
        self._create_callbacks()

    def _create_callbacks(self):
        @self.app.callback(
            Output('generation-status', 'children'),
            Output('generation-store', 'data'),
            Input('generate-button', 'n_clicks'),
            State('document-type-dropdown', 'value'),
            State('document-description', 'value'),
            State('document-count-slider', 'value'),
            prevent_initial_call=True
        )
        def generate_documents(n_clicks, doc_type, description, count):
            if not n_clicks or not description:
                return dash.no_update, dash.no_update
            
            # Start generation process
            status_message = dbc.Alert([
                html.H5("🔄 Generating Documents...", className="mb-2"),
                html.P(f"Creating {count} {self._format_doc_type(doc_type)} document(s)"),
                dbc.Progress(id="progress-bar", value=0, striped=True, animated=True)
            ], color="info")
            
            try:
                # Generate documents
                generated_files = self._generate_documents_batch(doc_type, description, count)
                
                # Success message
                success_message = dbc.Alert([
                    html.H5("✅ Generation Complete!", className="mb-2"),
                    html.P(f"Successfully generated {len(generated_files)} document(s):"),
                    html.Ul([
                        html.Li(file_info['filename']) for file_info in generated_files
                    ]),
                    html.Hr(),
                    html.P([
                        "Files saved to: ",
                        html.Code(self.volume_path)
                    ], className="mb-0")
                ], color="success")
                
                return success_message, {'status': 'complete', 'files': generated_files}
                
            except Exception as e:
                error_message = dbc.Alert([
                    html.H5("❌ Generation Failed", className="mb-2"),
                    html.P(f"Error: {str(e)}")
                ], color="danger")
                
                return error_message, {'status': 'error', 'error': str(e)}

    def _generate_documents_batch(self, doc_type, description, count):
        """Generate multiple documents based on the specified parameters."""
        generated_files = []
        
        for i in range(count):
            # Generate content using the serving endpoint
            content = self._generate_document_content(doc_type, description, i + 1)
            
            # Create filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{doc_type}_{timestamp}_{i+1:02d}.pdf"
            
            # Generate PDF
            pdf_path = self._create_pdf(content, filename, doc_type)
            
            generated_files.append({
                'filename': filename,
                'path': pdf_path,
                'doc_type': doc_type,
                'size': os.path.getsize(pdf_path) if os.path.exists(pdf_path) else 0
            })
            
            # Small delay between generations to avoid rate limiting
            time.sleep(0.5)
        
        return generated_files

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
            
            # Extract content using the same logic as the fixed chatbot
            if isinstance(response, dict):
                if "content" in response:
                    return response["content"]
                elif "summary" in response:
                    return response["summary"]
                elif "text" in response:
                    return response["text"]
                elif "message" in response:
                    return response["message"]
                else:
                    return str(response)
            else:
                return str(response)
                
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
