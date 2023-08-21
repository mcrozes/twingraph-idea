import uuid
from jinja2 import Template
import base64
import subprocess
import pathlib
import sys
import os
import json
import re

OPENPBS_JOB_BIN_FOLDER = "/opt/pbs/bin"
QSUB = f"{OPENPBS_JOB_BIN_FOLDER}/qsub"
QSTAT = f"{OPENPBS_JOB_BIN_FOLDER}/qstat"
QDEL = f"{OPENPBS_JOB_BIN_FOLDER}/qdel"
QALTER = f"{OPENPBS_JOB_BIN_FOLDER}/qalter"


def validate_openpbs_binary() -> bool:
    # will verify if qstat/qdel/sub etc are available on the system
    for executable in [QSUB, QSTAT, QDEL, QALTER]:
        if not pathlib.Path(executable).is_file():
            print(f"{executable} not found. Update OpenPBS bin location if needed.")
            return False
    return True


def run_cmd(cmd, shell=True) -> dict:
    print(f"Running {cmd}")
    _result = subprocess.run(args=cmd, shell=shell, capture_output=True, text=True)
    return {
        "return_code": _result.returncode,
        "stdout": _result.stdout,
        "stderr": _result.stderr,
    }


# https://docs.ide-on-aws.com/hpc-simulations/user-documentation/supported-ec2-parameters
# https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/
def submit_hpc_job(
    job_body: base64.b64encode,
    nodes_count: int,
    depend: str = False,  # Job dependency, must be after, afterok, afternotok or afterany
    depend_job_ids: str = False,  # Job ID(s) dependency when depend parameter is set. ex: afterok:2 -> Job will be executed if job id 2 completed successfully.
    base_os: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#base_os
    efa_support: bool = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#efa_support
    force_ri: bool = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#force_ri
    fsx_lustre: [
        bool,
        str,
    ] = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#fsx_lustre
    fsx_lustre_size: int = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#fsx_lustre_size
    fsx_lustre_deployment_type: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#fsx_lustre_deployment_type
    ht_support: bool = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#ht_support_1
    instance_ami: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#instance_ami
    instance_profile: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#instance_profile
    instance_type: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#instance_type
    job_name: str = False,  # Name of the job. Random uuid will be used if False
    job_stdout_location: str = False,  # Absolute Path to the stdout file. Created automatically if False.
    job_stderr_location: str = False,  # Absolute Path to the stderr file. Created automatically if False
    keep_ebs: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#keep_ebs
    placement_group: bool = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#placement_group
    queue: str = False,  # HPC Queue to use, will use IDEA default if not set
    root_size: int = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#root_size
    scratch_iops: int = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#scratch_iops
    scratch_size: int = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#scratch_size
    security_groups: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#security_groups
    spot_allocation_count: int = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#spot_allocation_count
    spot_allocation_strategy: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#spot_allocation_strategy
    spot_price: [
        str,
        int,
    ] = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#spot_price
    subnet_id: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#subnet_id
    idea_endpoint: str = os.environ.get(
        "TWINGRAPH_IDEA_ENDPOINT", False
    ),  # IDEA URL in order to submit HPC job remotely via HTTP APIs. If False, we assume TwinGraph is installed locally on an IDEA cluster
    idea_username: str = os.environ.get(
        "TWINGRAPH_IDEA_USERNAME", False
    ),  # IDEA Username in order to submit HPC job remotely via HTTP APIs. Required if idea_endpoint is set
    idea_password: str = os.environ.get(
        "TWINGRAPH_IDEA_PASSWORD", False
    ),  # IDEA Password in order to submit HPC job remotely via HTTP APIs. Required if idea_endpoint is set
) -> dict:
    _openpbs_raw_job_template_data = locals()

    # Validate OpenPBS bin
    if not validate_openpbs_binary():
        sys.exit(1)

    if idea_endpoint and (not idea_username and not idea_password):
        print("IDEA Username/Password must be specified when IDEA endpoint is enabled.")
        sys.exit(1)
    if idea_username or idea_password and not idea_endpoint:
        print("`idea_endpoint` must be specified")
        sys.exit(1)

    _allowed_job_dependency = ["after", "afterok", "afternotok", "afterany", False]
    if depend not in _allowed_job_dependency:
        print(f"'depend' must be in {_allowed_job_dependency}, detected {depend}")
        sys.exit(1)

    if (depend and not depend_job_ids) or (depend_job_ids and not depend):
        print(
            f"'depend_job_ids' and 'depend' must be specified together. Detected depend: {depend}, depend_job_id: {depend_job_ids}"
        )
        sys.exit(1)

    # Retrieve function parameters and add new as needed
    # Note: parameters not set will inherit the default queue value
    # https://awslabs.github.io/scale-out-computing-on-aws/web-interface/create-your-own-queue/#option1-i-want-to-use-the-same-settings-as-an-existing-queue
    _job_uuid = str(uuid.uuid4())
    _openpbs_raw_job_template_data["job_body_decoded"] = base64.b64decode(
        _openpbs_raw_job_template_data["job_body"]
    ).decode("utf-8")
    if not _openpbs_raw_job_template_data["job_name"]:
        _openpbs_raw_job_template_data["job_name"] = _job_uuid
    _current_working_directory = pathlib.Path.cwd()
    _job_script_name = f"{_current_working_directory}/{_job_uuid}.qsub"
    if not job_stdout_location:
        _openpbs_raw_job_template_data[
            "job_stdout_location"
        ] = f"{_current_working_directory}/{_job_uuid}.stdout"
    if not job_stderr_location:
        _openpbs_raw_job_template_data[
            "job_stderr_location"
        ] = f"{_current_working_directory}/{_job_uuid}.stderr"

    # Job Template
    _openpbs_raw_job_template = """
############################
# Generic OpenPBS parameters
# Add any additional PBS parameters as needed
############################

{% if data.queue %}
#PBS -q {{ data.queue }}
{% endif %}

#PBS -N {{ data.job_name }}
#PBS -e {{ data.job_stderr_location}}
#PBS -o {{ data.job_stdout_location}}

{% if data.depend %}
#PBS -W depend={{ data.depend }}:{{data.depend_job_ids}}
{% endif %}
############################
# SOCA/IDEA Scheduler Parameters
# https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/
############################


{% for key,value in data.items() %}
  {% if key in ('base_os',
  'efa_support',
  'force_ri',
  'fsx_lustre',
  'fsx_lustre_size',
  'fsx_lustre_deployment_type',
  'ht_support',
  'instance_ami',
  'instance_profile',
  'instance_type',
  'keep_ebs',
  'placement_group',
  'root_size',
  'scratch_iops',
  'scratch_size',
  'security_groups',
  'spot_allocation_count',
  'spot_allocation_strategy',
  'spot_price',
  'subnet_id') and value %}
#PBS -l {{key}}={{ value }}  
  {% endif %}
{% endfor %}

############################
# Actual Job Command
############################
{{ data.job_body_decoded }}"""

    _job_template = Template(_openpbs_raw_job_template)
    _unformatted_job_script = _job_template.render(data=_openpbs_raw_job_template_data)
    _sanitized_job_script = "".join(
        [s for s in _unformatted_job_script.strip().splitlines(True) if s.strip()]
    )

    if not idea_endpoint:
        # No IDEA Username/Password specified, assuming TwinGraph is installed on IDEA and have access to qsub binary
        try:
            with open(_job_script_name, "w") as text_file:
                text_file.write(_sanitized_job_script)
        except FileNotFoundError as err:
            print(
                f"Unable to create {_job_script_name}. You most likely don't have write permission to this location. Trace {err}"
            )
            sys.exit(1)
        except Exception as err:
            print(f"Unable to create {_job_script_name} due to {err}")
            sys.exit(1)

        print(f"Successfully created {_job_script_name}")

        _submit_job = run_cmd(f"{QSUB} {depend if depend else ''} {_job_script_name}")
        # _submit_job = run_cmd(f"ls -ltr")

        if int(_submit_job["return_code"]) == 0:
            _job_id = _submit_job["stdout"].rstrip().lstrip()
            print(f"Job {_job_id} submitted successfully.")
        else:
            print(f"Unable to submit job due to {_submit_job}")
            _job_id = None
    else:
        pass
    return {
        "job_uuid": _job_uuid,
        "job_id": _job_id,
        "job_stdout_location": _openpbs_raw_job_template_data["job_stdout_location"],
        "job_stderr_location": _openpbs_raw_job_template_data["job_stderr_location"],
        "qsub": _submit_job,
    }


def get_job_output_file(job_output_path: str) -> dict:
    _success = False
    try:
        with open(job_output_path, "r") as f:
            _message = f.read()
            _success = True
    except FileNotFoundError as err:
        _message = f"Unable to read {job_output_path}. You most likely don't have read permission to this location or this file does not exist. Trace {err}"

    except Exception as err:
        _message = f"Unable to read {job_output_path} due to {err}"

    return {"success": _success, "message": _message}


def get_job_info(job_id: int) -> dict:
    _get_job_info = run_cmd(f"{QSTAT} -f {job_id} -Fjson -x")
    if _get_job_info["return_code"] == 0:
        _qstat_output = json.loads(_get_job_info["stdout"])

        # expected key is <job_id>-<scheduler-ip>. Most of the time we expect users to simply pass the job id.
        # Below line is to automatically reformat job id into jobid-scheduler-ip

        _job_key = list(_qstat_output["Jobs"].keys())
        return {
            "success": True,
            "job_state": _qstat_output["Jobs"][_job_key[0]]["job_state"],
            "message": _qstat_output,
        }
    else:
        return {
            "success": False,
            "job_state": False,
            "message": f"Unable to get qstat output for {job_id}. Trace {_get_job_info}",
        }
