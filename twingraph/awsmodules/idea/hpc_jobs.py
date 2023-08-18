import uuid
from jinja2 import Template
import base64
import subprocess
import pathlib
import sys

# //print statement must be replaced with logging()

OPENPBS_JOB_BIN_FOLDER = "/opt/bin"
QSUB = f"{OPENPBS_JOB_BIN_FOLDER}/qsub"
QSTAT = f"{OPENPBS_JOB_BIN_FOLDER}/qstat"
QDEL = f"{OPENPBS_JOB_BIN_FOLDER}/qdel"
QALTER = f"{OPENPBS_JOB_BIN_FOLDER}/qalter"


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
# For this PoC, I'm not planning to add all EC2 parameters supported by SOCA/IDEA
def submit_hpc_job(
    job_body: base64.b64encode,
    nodes_count: int,
    base_os: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#base_os
    efa_support: bool = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#efa_support
    force_ri: bool = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#force_ri
    fsx_lustre: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#fsx_lustre
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
    queue: str = False,  # HPC Queue to use, will use default if not set
    root_size: int = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#root_size
    scratch_iops: int = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#scratch_iops
    scratch_size: int = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#scratch_size
    security_groups: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#security_groups
    spot_allocation_count: int = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#spot_allocation_count
    spot_allocation_strategy: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#spot_allocation_strategy
    spot_price: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#spot_price
    subnet_id: str = False,  # https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/#subnet_id
) -> dict:

    # Retrieve function parameters and add new as needed
    # Note: parameters not set will inherit the default queue value
    # https://awslabs.github.io/scale-out-computing-on-aws/web-interface/create-your-own-queue/#option1-i-want-to-use-the-same-settings-as-an-existing-queue
    _job_uuid = str(uuid.uuid4())
    _openpbs_raw_job_template_data = locals()
    _openpbs_raw_job_template_data["job_body_decoded"] = base64.b64decode(
        _openpbs_raw_job_template_data["job_body"]
    ).decode("utf-8")
    if not _openpbs_raw_job_template_data["job_name"]:
        _openpbs_raw_job_template_data["job_name"] = _job_uuid
    _current_working_directory = pathlib.Path.cwd()
    _job_script_name = f"{_current_working_directory}/{_job_uuid}.qsub"
    if not job_stdout_location:
        job_stdout_location = f"{_current_working_directory}/{_job_uuid}.stdout"
    if not job_stderr_location:
        job_stderr_location = f"{_current_working_directory}/{_job_uuid}.stderr"

    # Job Template
    _openpbs_raw_job_template = """
############################
# Generic OpenPBS parameters
# Add any additional PBS parameters as needed
############################

#PBS -q {{ data.queue }}
#PBS -n {{ data.job_name }}
#PBS -e {{ data.job_stderr_location}}
#PBS -o {{ data.job_stdout_location}}

############################
# SOCA/IDEA Scheduler Parameters
# https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/
############################

{% for key,value in data.items() %}
  {% if key not in ("nodes_count", "queue", "job_name", "job_body_decoded", "job_body") and value %}
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

    # Save job script

    try:
        with open(_job_script_name, "w") as text_file:
            text_file.write(_sanitized_job_script)
    except FileNotFoundError as err:
        print(
            f"Unable to create {_job_script_name}. You most likely don't have write permission to this location"
        )
        sys.exit(1)
    except Exception as err:
        print(f"Unable to create {_job_script_name} due to {err}")
        sys.exit(1)

    print(f"Successfully created {_job_script_name}")

    # _submit_job = run_cmd(f"{QSUB} {job_script_name}")
    _submit_job = run_cmd(f"ls -ltr")

    if int(_submit_job["return_code"]) == 0:
        _job_id = _submit_job["stdout"].rstrip().lstrip()
    else:
        _job_id = None

    return {
        "job_uuid": _job_uuid,
        "job_id": _job_id,
        "job_stdout": job_stdout_location,
        "job_stderr": job_stderr_location,
        "qsub": _submit_job,
    }


job_command = b"""
ls -ltr
pwd
[[ if $? -ne 0 ]]; then
  echo "test if"
fi
"""

z = submit_hpc_job(
    job_body=base64.b64encode(job_command),
    job_name="testjob",
    nodes_count=5,
    queue="test",
    scratch_size=50,
)

print(z)
