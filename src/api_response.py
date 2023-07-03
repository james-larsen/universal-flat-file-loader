from pathlib import Path
import re
# import json
from flask import make_response, jsonify


def convert_paths_to_strings(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                convert_paths_to_strings(value)
            elif isinstance(value, Path):
                data[key] = re.sub('\\\\', '/', str(value))
    return data

class ApiResponse:
    api_response = None
    job_errors_or_warnings = 0
    
    def __init__(self):
        self.api_result = {}
        self.api_error_flag = None
        self.api_message = None
        self.log_file_path = None

    # def add_result(self, action, api_result):
    #     self.results.append(api_result)

    # def get_results(self):
    #     return self.api_result

    # def reset(self):
    #     self.api_result = {}

    def build_api_response(self, app_context, function, job_errors_or_warnings=0):#, my_settings):
        # global my_settings, my_api_response#, api_result, api_error_flag, api_message
        
        self.job_errors_or_warnings = job_errors_or_warnings

        with app_context:
            if self.api_error_flag:
                # if not self.api_result:
                #     self.api_result = {}
                response_data = {
                    'function': function,
                    'status': 'error',
                    'errors_or_warnings': self.job_errors_or_warnings,
                    'log_file_path': str(self.log_file_path),
                    'message': self.api_message,
                    'result': {}
                }
                api_response = make_response(jsonify(response_data), 400)
            else:
                if not self.api_result:
                    result = {}
                else:
                    result = self.api_result
                if not self.api_message:
                    self.api_message = 'Completed successfully'
                cleansed_result = convert_paths_to_strings(result)
                response_data = {
                    'function': function,
                    'status': 'success',
                    'errors_or_warnings': self.job_errors_or_warnings,
                    'log_file_path': str(self.log_file_path),
                    'message': self.api_message,
                    'result': cleansed_result
                }
                # json_string = json.dumps(response_data, sort_keys=False)
                api_response = make_response(jsonify(response_data), 200)

            api_response.headers['Content-Type'] = 'application/json'
            # Decode with: "json.loads(api_response.get_data(as_text=True))"
            # Will be different when reading the results from another application

            self.api_response = api_response
