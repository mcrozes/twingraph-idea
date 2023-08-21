import twingraph.awsmodules.idea.hpc_jobs as idea_hpc
import base64
import time

# Local tests for now, must be added to Celery

# All Supported Parameters
# https://docs.ide-on-aws.com/hpc-simulations/user-documentation/supported-ec2-parameters
# https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/

# Standard HPC job on the "normal" queue using ALI2
job_standard = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(b"expr 1 + 1"),
    base_os="amazonlinux2",
    job_name="TestJob",
    queue="normal",
    nodes_count=1,
    instance_type="t3.2xlarge",
)

# idea_hpc.submit_hpc_job() returns a dictionary. See Example below
#         {
#         	'job_uuid': 'dda015c3-8768-4725-86fc-25a8e0e1e901',
#         	'job_id': '1.ip-60-0-114-81',
#         	'job_stdout_location': '/data/home/mcrozes/dda015c3-8768-4725-86fc-25a8e0e1e901.stdout',
#         	'job_stderr_location': /data/home/mcrozes/dda015c3-8768-4725-86fc-25a8e0e1e901.stderr,
#         	'qsub': {
#         		'return_code': 0,
#         		'stdout': '1.ip-60-0-114-81\n',
#         		'stderr': ''
#         	}
#         }
#

# Run the following job only if previous job was queued successfully
# Note: depend/depend_job_ids are optional here if you perform the output check in Python directly.
# Another option is to ignore the return_code/output check and simply queue them at the same time
# In this setup, this job will only be executed if the previous job ran successfully ("afterok:job_id").
# The only difference here is this job will stay in the queue forever in H (held) state if the previous job did not run successfully.


if job_standard["qsub"]["return_code"] == 0:
    job_with_dependency = idea_hpc.submit_hpc_job(
        job_body=base64.b64encode(
            b'echo "Job run if standard job was queued successfully'
        ),
        job_name="RunAfterStandard",
        depend="afterok",
        depend_job_ids=job_standard["job_id"],
        nodes_count=1,
        instance_type="t3.2xlarge",
    )

# Additionally, in addition of return code, you can browse the job output and decide to run the next job only if:
# - the previous job(s) was queued successfully
# - the previous job(s) ran successfully and produced the expected output

if job_standard["qsub"]["return_code"] == 0:
    # Wait until the job has completed
    if not idea_hpc.get_job_info(job_standard["job_id"])["success"]:
        print(f"Unable to get job information for {job_standard}")
    else:
        while idea_hpc.get_job_info(job_standard["job_id"])["job_state"] not in [
            "F",
            "E",
        ]:
            time.sleep(60)

        # Job has completed, checking output
        job_output_content = idea_hpc.get_job_output_file(
            job_standard["job_stdout_location"]
        )

        # Verify if the previous job produced the stdout file
        if job_output_content["success"]:
            # Determine next steps based on job output
            if int(job_output_content) == 2:
                job_with_dependency_alt = idea_hpc.submit_hpc_job(
                    job_body=base64.b64encode(
                        b'echo "Job run if standard job completed successfully and produced expected output'
                    ),
                    job_name="RunAfterStandard",
                    depend="afterok",
                    depend_job_ids=job_standard["job_id"],
                    nodes_count=1,
                    instance_type="t3.2xlarge",
                )


# Other examples

# Standard HPC job using CentOS and different instance type
job_standard_centos = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(b'echo "Standard CentOS HPC job running on IDEA"'),
    base_os="centos7",
    job_name="centosjob",
    queue="normal",
    nodes_count=1,
    instance_type="m5.large",
)

# Standard HPC job using CentOS with fallback EC2 instance type
# In this example job will use m5.large if available, otherwise will try to provision c5.large or r5.large
# Note: this job also don't have job_name, a uuid will be generated
job_standard_fallback_ec2 = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(b'echo "FallBack EC2 instance"'),
    nodes_count=1,
    instance_type="m5.large+c5.large+r5.large",
)

# Spot job with spot price set to auto (=max on-demand price)
job_spot = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(b'echo "Spot Job"'),
    job_name="spot",
    spot_price="auto",
    nodes_count=1,
    instance_type="m5.large",
)

# Job with 50 GB / partition and extra 90GB for /scratch partition
job_extra_storage = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(b"df -h"),
    job_name="JobExtraStorage",
    nodes_count=1,
    instance_type="t3.2xlarge",
    root_size=50,
    scratch_size=90,
)

# Job running on c5n with EFA automatically installed
job_efa = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(b"fi_info -p efa -t FI_EP_RDM"),
    job_name="TestJob",
    nodes_count=1,
    instance_type="c5n.18xlarge",
    efa_support=True,
)


job_mpi = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(
        b"/apps/openmpi/4.1.5/bin/mpirun --host $(cat $PBS_NODEFILE | sort | uniq) hostname"
    ),
    job_name="MPI",
    nodes_count=3,
    instance_type="t3.2xlarge",
)

"""
print(job_standard)
print(job_standard_centos)
print(job_standard_fallback_ec2)
print(job_spot)
print(job_extra_storage)
print(job_efa)
print(job_with_dependency)
print(job_mpi)
"""
