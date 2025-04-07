from ray.job_submission import JobStatus

DEFAULT_K8S_NAMESPACE = "default"
TERMINAL_JOB_STATUSES = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}
