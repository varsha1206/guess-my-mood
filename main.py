import base64
import json
import traceback
from google.cloud import dataproc_v1

def trigger_dataproc(event, context):
    """Triggered from a message on a Pub/Sub topic."""
    try:
        # Log the incoming event for debugging
        print(f"Event received: {event}")
        print(f"Context: {context}")
        
        # Decode Pub/Sub message if it exists
        if 'data' in event:
            message_data = base64.b64decode(event['data']).decode('utf-8')
            print(f"Message data: {message_data}")
        
        project_id = "dm2-v-479909"
        region = "us-central1"
        workflow_template_id = "mood-pipeline-template"

        client = dataproc_v1.WorkflowTemplateServiceClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

        template_name = client.workflow_template_path(
            project_id, region, workflow_template_id
        )
        print(f"Template name: {template_name}")
        
        # Instantiate the workflow
        operation = client.instantiate_workflow_template(
            request={"name": template_name}
        )
        print(f"Workflow triggered. Operation: {operation.operation.name}")
        
        return "Workflow triggered successfully"
        
    except Exception as e:
        error_msg = f"Error triggering Dataproc workflow: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        raise RuntimeError(error_msg)