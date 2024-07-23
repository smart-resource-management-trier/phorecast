import argparse
import os
import shutil
import jinja2
import secrets
from src.utils.static import docker_build_path
# Static
COPY_FILES = ["grafana/provisioning/dashboards/DefaultDashboard.json",
              "grafana/provisioning/dashboards/DefaultDashboard.yml",
              "compose.yml",
              "nginx/default.conf.template",
              "grafana/grafana.ini",
              "grafana/provisioning/datasources/DefaultDatasource.yml"]


PARSE_FILES = [".env","grafana/provisioning/datasources/DefaultDatasource.yml"]

# Arguments
parser = argparse.ArgumentParser(description='This Project depends on a software stack which '
                                             'utilizes docker compose. This build script will '
                                             'create a complete docker-compose configuration ready '
                                             'to be deployed.')

parser.add_argument('--output', '-o', type=str,
                    help='Folder to write create the configuration in', required=True)
parser.add_argument('--deployment_name', '-n', type=str,
                    help='Name of the deployment (used by docker)', default='phorecast')
parser.add_argument('--host', '-u', type=str,
                    help='Host name of the server', default='localhost')
parser.add_argument("--cert", "-c", type=str,
                    help="Path to the certificate file", required=True)
parser.add_argument("--key", "-k", type=str,
                    help="Path to the key file",
                    required=True)

parser = parser.parse_args()

target_path = parser.output
deployment_name = parser.deployment_name
host = parser.host
cert = parser.cert
key = parser.key

os.makedirs(target_path, exist_ok=True)

if os.listdir(target_path):
    print("The path given for the configuration is not empty. Please provide an empty folder: ",
          target_path)
    exit(1)

for file in COPY_FILES:
    source_path = os.path.join(docker_build_path, file)
    destination_path = os.path.join(target_path, file)
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
    shutil.copy(source_path, destination_path)
    print("writing file: ", destination_path)

context = {
    "influx_token": secrets.token_hex(32),
    "domain_name": host,
    "cert_path": cert,
    "key_path": key,
    "flask_key": secrets.token_hex(32),
}

for file in PARSE_FILES:
    with open(os.path.join(docker_build_path, file), 'r') as env_file:
        rendered_content = jinja2.Template(env_file.read()).render(context)

    # Save the rendered content to the output file
    with open(os.path.join(target_path,file), 'w') as output_file:
        output_file.write(rendered_content)
        print("writing file: ", os.path.join(target_path,file))