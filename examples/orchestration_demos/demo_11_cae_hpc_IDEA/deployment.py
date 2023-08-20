import twingraph.awsmodules.idea.hpc_jobs as idea_hpc
import base64

# Local tests for now, must be added to Celery

# All Supported Parameters
# https://docs.ide-on-aws.com/hpc-simulations/user-documentation/supported-ec2-parameters
# https://awslabs.github.io/scale-out-computing-on-aws/tutorials/integration-ec2-job-parameters/

# Standard HPC job on the "normal" queue using ALI2
job_standard = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(b'echo "Standard HPC job running on IDEA"'),
    base_os="amazonlinux2",
    job_name="TestJob",
    queue="normal",
    nodes_count=1,
    instance_type="t3.2xlarge",
)

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


job_with_dependency = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(b'echo "Job will be executed after Standard HPC Job'),
    job_name="RunAfterStandard",
    depend="afterok",
    depend_job_ids=job_standard["job_id"],
    nodes_count=1,
    instance_type="t3.2xlarge",
)

job_mpi = idea_hpc.submit_hpc_job(
    job_body=base64.b64encode(
        b"/apps/openmpi/4.1.5/bin/mpirun --host $(cat $PBS_NODEFILE | sort | uniq) hostname"
    ),
    job_name="MPI",
    nodes_count=3,
    instance_type="t3.2xlarge",
)

print(job_standard)
print(job_standard_centos)
print(job_standard_fallback_ec2)
print(job_spot)
print(job_extra_storage)
print(job_efa)
print(job_with_dependency)
print(job_mpi)
